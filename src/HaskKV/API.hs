module HaskKV.API
    ( RaftContext (..)
    , api
    , server
    ) where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad.Except
import Data.Proxy
import HaskKV.Log.Entry
import HaskKV.Log.Utils (apply)
import HaskKV.Server
import HaskKV.Store
import Servant.API
import Servant.Server

type StoreAPI k v
    = "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
 :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
 :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

data RaftContext k v msg = RaftContext
    { _store       :: TVar (Store k v (LogEntry k v))
    , _isLeader    :: TVar Bool
    , _serverState :: ServerState msg
    }

api :: Proxy (StoreAPI k v)
api = Proxy

get :: (KeyClass k, ValueClass v) => RaftContext k v msg -> k -> Handler (Maybe v)
get context key =
    checkLeader context $ runStoreTVar (getValue key) (_store context)

set :: RaftContext k v msg -> k -> v -> Handler ()
set context key value =
    checkLeader context $ applyEntryData context entryData
  where
    entryData = Change (TID 0) key value

delete :: RaftContext k v msg -> k -> Handler ()
delete context key =
    checkLeader context $ applyEntryData context entryData
  where
    entryData = Delete (TID 0) key

server :: (KeyClass k, ValueClass v)
       => RaftContext k v msg
       -> Server (StoreAPI k v)
server context = get context :<|> set context :<|> delete context

checkLeader :: RaftContext k v msg -> Handler r -> Handler r
checkLeader context handler =
    (liftIO . readTVarIO . _isLeader) context >>= \case
        True  -> handler
        False -> throwError err404

applyEntryData :: RaftContext k v msg -> LogEntryData k v -> Handler ()
applyEntryData context entryData = do
    completed <- Completed . Just <$> liftIO newEmptyTMVarIO
    let entry = LogEntry
            { _term      = 0
            , _index     = 0
            , _data      = entryData
            , _completed = completed
            }
    f <- liftIO $ async $ runStoreTVar (apply entry) (_store context)
    runServerT (inject HeartbeatTimeout) (_serverState context)
    liftIO $ wait f
