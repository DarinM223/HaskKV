module HaskKV.Monad where

import Control.Lens (lens, (^.))
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Binary.Orphans ()
import Data.Generics.Product.Fields
import Data.IORef
import GHC.Generics
import HaskKV.Constr
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.Class
import HaskKV.Raft.State
import HaskKV.Server.All
import HaskKV.Snapshot.All
import HaskKV.Store.All

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
  { initLog       :: Maybe (Log e)
  , initState     :: Maybe PersistentState
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
  let serverState = config ^. field @"serverState"
      sid         = serverState ^. field @"_sid"
      raftState   = newRaftState sid $ config ^. field @"initState"
  raftStateRef <- newIORef raftState
  store        <- newStore sid $ config ^. field @"initLog"
  tempLog      <- newTempLog
  snapManager  <- newSnapshotManager $ config ^. field @"snapDirectory"
  let config = AppConfig
        { cState       = raftStateRef
        , cStore       = store
        , cTempLog     = tempLog
        , cServerState = serverState
        , cSnapManager = snapManager
        , cRun         = flip runApp config
        }
  return config
