module HaskKV.API
  ( api
  , server
  )
where

import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM (newEmptyTMVarIO)
import Control.Monad.Except
import Control.Monad.State (MonadState, gets)
import Data.Aeson (FromJSON, ToJSON)
import Data.IORef
import Data.Proxy (Proxy(Proxy))
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.Temp (waitApplyEntry)
import HaskKV.Monad
import HaskKV.Raft.State
import HaskKV.Server.All
import HaskKV.Store.Types
import Servant.API
import Servant.Server

type Run m = forall a. m a -> IO a

type StoreAPI k v
  =    "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
  :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
  :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

api :: Proxy (StoreAPI k v)
api = Proxy

get :: MonadState RaftState m
    => StorageM k v m -> k -> ExceptT ServantErr m (Maybe v)
get storeM key = checkLeader $ getValue storeM key

set :: (MonadState RaftState m, MonadIO m)
    => ServerM msg ServerEvent m
    -> TempLogM (LogEntry k v) m
    -> Run m
    -> k
    -> v
    -> ExceptT ServantErr m ()
set serverM tempLogM run key value =
  checkLeader $ applyEntryData serverM tempLogM run $ Change (TID 0) key value

delete :: (MonadState RaftState m, MonadIO m)
       => ServerM msg ServerEvent m
       -> TempLogM (LogEntry k v) m
       -> Run m
       -> k
       -> ExceptT ServantErr m ()
delete serverM tempLogM run key = checkLeader $
  applyEntryData serverM tempLogM run $ Delete (TID 0) key

isLeader :: MonadState RaftState m => m Bool
isLeader = gets _stateType >>= \case
  Leader _ -> return True
  _        -> return False

checkLeader :: MonadState RaftState m => m a -> ExceptT ServantErr m a
checkLeader handler = lift isLeader >>= \case
  True  -> lift handler
  False -> throwError err404

convertApp :: Run m -> ExceptT ServantErr m a -> Handler a
convertApp run = Handler . ExceptT . run . runExceptT

server
  :: ( KeyClass k, ValueClass v, FromHttpApiData k, FromJSON v, ToJSON v
     , MonadState RaftState m, MonadIO m )
  => StorageM k v m
  -> ServerM msg ServerEvent m
  -> TempLogM (LogEntry k v) m
  -> Run m
  -> Server (StoreAPI k v)
server storeM serverM tempLogM run = hoistServer api (convertApp run) server
 where
  server = get storeM
      :<|> set serverM tempLogM run
      :<|> delete serverM tempLogM run

applyEntryData :: MonadIO m
               => ServerM msg ServerEvent m
               -> TempLogM (LogEntry k v) m
               -> Run m
               -> LogEntryData k v
               -> m ()
applyEntryData serverM tempLogM run entryData = do
  completed <- Completed . Just <$> liftIO newEmptyTMVarIO
  let entry = LogEntry
        { _term      = 0
        , _index     = 0
        , _data      = entryData
        , _completed = completed
        }
  f <- liftIO . async . run $ waitApplyEntry tempLogM entry
  inject serverM HeartbeatTimeout
  liftIO $ wait f
