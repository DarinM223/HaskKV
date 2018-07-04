module HaskKV.API
    ( api
    , server
    ) where

import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad.Except
import Data.Proxy
import GHC.Records
import HaskKV.Log.Entry
import HaskKV.Log.Temp (waitApplyEntry)
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

get :: (KeyClass k) => AppConfig msg k v e -> k -> Handler (Maybe v)
get config key =
    checkLeader config $ liftIO $ getValueImpl key (_store config)

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

server :: (KeyClass k)
       => AppConfig msg k v (LogEntry k v)
       -> Server (StoreAPI k v)
server config = get config :<|> set config :<|> delete config

checkLeader :: AppConfig msg k v e -> Handler r -> Handler r
checkLeader config handler =
    (liftIO . isLeader . _state) config >>= \case
        True  -> handler
        False -> throwError err404

applyEntryData :: AppConfig msg k v (LogEntry k v)
               -> LogEntryData k v
               -> Handler ()
applyEntryData config entryData = liftIO $ do
    completed <- Completed . Just <$> newEmptyTMVarIO
    let entry = LogEntry
            { _term      = 0
            , _index     = 0
            , _data      = entryData
            , _completed = completed
            }
    f <- async $ waitApplyEntry entry (_tempLog config)
    inject HeartbeatTimeout $ getField @"_serverState" config
    wait f
