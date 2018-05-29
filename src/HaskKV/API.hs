module HaskKV.API
    ( api
    , server
    ) where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad.Except
import Data.Proxy
import HaskKV.Log.Entry
import HaskKV.Log.Utils (apply)
import HaskKV.Monad
import HaskKV.Server
import HaskKV.Store
import Servant.API
import Servant.Server

type StoreAPI k v
    = "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
 :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
 :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

api :: Proxy (StoreAPI k v)
api = Proxy

get :: (KeyClass k, ValueClass v)
    => AppConfig msg k v e
    -> k
    -> Handler (Maybe v)
get config key =
    checkLeader config $ liftIO $ runAppTConfig (getValue key) config

set :: AppConfig msg k v (LogEntry k v) -> k -> v -> Handler ()
set config key value =
    checkLeader config $ applyEntryData config entryData
  where
    entryData = Change (TID 0) key value

delete :: AppConfig msg k v (LogEntry k v) -> k -> Handler ()
delete config key =
    checkLeader config $ applyEntryData config entryData
  where
    entryData = Delete (TID 0) key

server :: (KeyClass k, ValueClass v)
       => AppConfig msg k v (LogEntry k v)
       -> Server (StoreAPI k v)
server config = get config :<|> set config :<|> delete config

checkLeader :: AppConfig msg k v e -> Handler r -> Handler r
checkLeader config handler =
    (liftIO . readTVarIO . _isLeader) config >>= \case
        True  -> handler
        False -> throwError err404

applyEntryData :: AppConfig msg k v (LogEntry k v)
               -> LogEntryData k v
               -> Handler ()
applyEntryData config entryData = do
    completed <- Completed . Just <$> liftIO newEmptyTMVarIO
    let entry = LogEntry
            { _term      = 0
            , _index     = 0
            , _data      = entryData
            , _completed = completed
            }
    f <- liftIO $ async $ runAppTConfig (apply entry) config
    liftIO $ runAppTConfig (inject HeartbeatTimeout) config
    liftIO $ wait f
