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
import HaskKV.Constr (Constr)
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

api :: Proxy (StoreAPI k v)
api = Proxy

get
  :: (Constr k v e m, MonadError ServantErr m)
  => k
  -> AppT msg k v e m (Maybe v)
get key = checkLeader $ getValue key

set
  :: (MonadIO m, MonadError ServantErr m)
  => k
  -> v
  -> AppT msg k v (LogEntry k v) m ()
set key value = checkLeader $ applyEntryData $ Change (TID 0) key value

delete
  :: (MonadIO m, MonadError ServantErr m)
  => k
  -> AppT msg k v (LogEntry k v) m ()
delete key = checkLeader $ applyEntryData $ Delete (TID 0) key

isLeader :: (MonadIO m) => AppT msg k v e m Bool
isLeader = do
  ref   <- asks _state
  state <- liftIO $ readIORef ref
  case _stateType state of
    Leader _ -> return True
    _        -> return False

checkLeader
  :: (MonadIO m, MonadError ServantErr m)
  => AppT msg k v e m r
  -> AppT msg k v e m r
checkLeader handler = isLeader >>= \case
  True  -> handler
  False -> lift $ throwError err404

convertApp :: AppConfig msg k v e -> AppT msg k v e Handler a -> Handler a
convertApp = flip runAppT

server
  :: (KeyClass k, ValueClass v, FromHttpApiData k, FromJSON v, ToJSON v)
  => AppConfig msg k v (LogEntry k v)
  -> Server (StoreAPI k v)
server config = hoistServer api (convertApp config) server
  where server = get :<|> set :<|> delete

applyEntryData
  :: (MonadIO m) => LogEntryData k v -> AppT msg k v (LogEntry k v) m ()
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
