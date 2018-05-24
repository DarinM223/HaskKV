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
import HaskKV.Raft.State
import HaskKV.Store
import Servant.API
import Servant.Server

type StoreAPI k v
    = "get" :> Capture "key" k :> Get '[JSON] v
 :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
 :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

data RaftContext k v = RaftContext
    { _store     :: TVar (Store k v (LogEntry k v))
    , _raftState :: TVar RaftState
    }

api :: Proxy (StoreAPI k v)
api = Proxy

get :: (Show k, Ord k, Storable v) => RaftContext k v -> k -> Handler v
get context key = runStoreTVar (getValue key) (_store context) >>= \case
    Just value -> return value
    Nothing    -> throwError err404

set :: RaftContext k v -> k -> v -> Handler ()
set context key value = applyEntryData context entryData
  where
    entryData = Change (TID 0) key value

delete :: RaftContext k v -> k -> Handler ()
delete context key = applyEntryData context entryData
  where
    entryData = Delete (TID 0) key

server :: (Show k, Ord k, Storable v)
       => RaftContext k v
       -> Server (StoreAPI k v)
server context = get context :<|> set context :<|> delete context

applyEntryData :: RaftContext k v -> LogEntryData k v -> Handler ()
applyEntryData context entryData = do
    completed <- Completed . Just <$> liftIO newEmptyTMVarIO
    let entry = LogEntry
            { _term      = 0
            , _index     = 0
            , _data      = entryData
            , _completed = completed
            }
    f <- liftIO $ async $ runStoreTVar (apply entry) (_store context)
    liftIO $ wait f
