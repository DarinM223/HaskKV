module HaskKV.API
  ( api
  , server
  )
where

import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM (newEmptyTMVarIO)
import Control.Monad.Except
import Control.Monad.Reader
import Data.Aeson (FromJSON, ToJSON)
import Data.IORef
import Data.Proxy (Proxy(Proxy))
import GHC.Records
import HaskKV.Log.Entry
import HaskKV.Log.Temp (waitApplyEntry)
import HaskKV.Monad
import HaskKV.Raft.State
import HaskKV.Server.All
import HaskKV.Store.Types
import Servant.API
import Servant.Server

type StoreAPI k v
  =    "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
  :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
  :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

type MyHandler msg k v e a = ExceptT ServantErr (App msg k v e) a

api :: Proxy (StoreAPI k v)
api = Proxy

get :: StorageM k v m -> k -> ExceptT ServantErr m (Maybe v)
get storeM key = checkLeader $ getValue storeM key

set :: ApplyEntryM (LogEntry k v) m -> k -> v -> m ()
set (ApplyEntryM applyEntry) key value = checkLeader $ applyEntry $ Change (TID 0) key value

delete :: k -> MyHandler msg k v (LogEntry k v) ()
delete key = checkLeader $ applyEntryData $ Delete (TID 0) key

isLeader :: Monad m => m Bool
isLeader = do
  ref   <- asks _state
  state <- liftIO $ readIORef ref
  case _stateType state of
    Leader _ -> return True
    _        -> return False

checkLeader :: App msg k v e a -> MyHandler msg k v e a
checkLeader handler = lift isLeader >>= \case
  True  -> lift handler
  False -> throwError err404

convertApp :: AppConfig msg k v e -> MyHandler msg k v e a -> Handler a
convertApp config = Handler . ExceptT . flip runApp config . runExceptT

server
  :: (KeyClass k, ValueClass v, FromHttpApiData k, FromJSON v, ToJSON v)
  => AppConfig msg k v (LogEntry k v)
  -> Server (StoreAPI k v)
server config = hoistServer api (convertApp config) server
  where server = get :<|> set :<|> delete

applyEntryData :: LogEntryData k v -> App msg k v (LogEntry k v) ()
applyEntryData entryData = ask >>= \config -> liftIO $ do
  completed <- Completed . Just <$> newEmptyTMVarIO
  let
    entry = LogEntry
      { _term      = 0
      , _index     = 0
      , _data      = entryData
      , _completed = completed
      }
  f <- async $ _run config $ waitApplyEntry entry
  inject HeartbeatTimeout $ getField @"_serverState" config
  wait f
