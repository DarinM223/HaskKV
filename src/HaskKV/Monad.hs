{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeFamilies #-}
module HaskKV.Monad where

import Control.Monad.Reader (MonadIO (..), MonadReader, ReaderT (..))
import Control.Monad.State.Strict (MonadState (get, put))
import Data.Binary (Binary)
import Data.Binary.Instances ()
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import GHC.Generics (Generic)
import HaskKV.Constr (Fn, SnapshotType, HasRun (..), Constr)
import HaskKV.Log.Class (LogM, TempLogM)
import HaskKV.Log.Entry (LogEntry)
import HaskKV.Log.InMem (Log)
import HaskKV.Log.Temp
import HaskKV.Raft.Class
import HaskKV.Raft.State (PersistentState, RaftState, newRaftState)
import HaskKV.Server.All
import HaskKV.Snapshot.All
import HaskKV.Store.All
import Optics ((^.), lens)

data AppConfig msg k v e = AppConfig
  { cState       :: IORef RaftState
  , cStore       :: Store k v e
  , cTempLog     :: TempLog e
  , cServerState :: ServerState msg
  , cSnapManager :: SnapshotManager
  , cRun         :: Fn msg k v e
  }

instance HasServerState msg (AppConfig msg k v e) where
  serverStateL = lens cServerState (\s t -> s { cServerState = t })
instance HasStore k v e (AppConfig msg k v e) where
  storeL = lens cStore (\s t -> s { cStore = t })
instance HasTempLog e (AppConfig msg k v e) where
  tempLogL = lens cTempLog (\s t -> s { cTempLog = t })
instance HasSnapshotManager (AppConfig msg k v e) where
  snapshotManagerL = lens cSnapManager (\s t -> s { cSnapManager = t })
instance HasRun msg k v e (AppConfig msg k v e) where
  run = cRun

data InitAppConfig msg e = InitAppConfig
  { log           :: Maybe (Log e)
  , state         :: Maybe PersistentState
  , serverState   :: ServerState msg
  , snapDirectory :: Maybe FilePath
  } deriving Generic

instance MonadState RaftState (App msg k v e) where
  get = App $ ReaderT $ liftIO . readIORef . cState
  put x = App $ ReaderT $ liftIO . flip writeIORef x . cState

instance (Binary k, Binary v) =>
  HasSnapshotType (SnapshotType k v) (App msg k v e)

newtype App msg k v e a = App
  { unApp :: ReaderT (AppConfig msg k v e) IO a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader (AppConfig msg k v e) )
  deriving (ServerM msg ServerEvent) via ServerT (App msg k v e)
  deriving ( StorageM k v
           , LogM e
           , LoadSnapshotM (SnapshotType k v)
           , TakeSnapshotM ) via StoreT (App msg k v e)
  deriving (TempLogM e) via TempLogT (App msg k v e)
  deriving (SnapshotM (SnapshotType k v)) via SnapshotT (App msg k v e)
  deriving DebugM via PrintDebugT (App msg k v e)
  deriving PersistM via PersistT (App msg k v e)

deriving via StoreT (App msg k v e) instance (Constr k v e, e ~ LogEntry k v)
  => ApplyEntryM k v e (App msg k v e)

runApp :: App msg k v e a -> AppConfig msg k v e -> IO a
runApp m config = flip runReaderT config . unApp $ m

newAppConfig
  :: (KeyClass k, ValueClass v, e ~ LogEntry k v)
  => InitAppConfig msg e
  -> IO (AppConfig msg k v e)
newAppConfig config = do
  let serverState = config ^. #serverState
      sid         = serverState ^. #sid
      raftState   = newRaftState sid $ config ^. #state
  raftStateRef <- newIORef raftState
  store        <- newStore sid $ config ^. #log
  tempLog      <- newTempLog
  snapManager  <- newSnapshotManager $ config ^. #snapDirectory
  let config = AppConfig
        { cState       = raftStateRef
        , cStore       = store
        , cTempLog     = tempLog
        , cServerState = serverState
        , cSnapManager = snapManager
        , cRun         = (\app -> runApp app config)
        }
  return config
