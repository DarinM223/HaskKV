{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeFamilies #-}
module HaskKV.Monad where

import Control.Monad.Reader (MonadIO (..), MonadReader, ReaderT (..), asks, void)
import Control.Monad.State.Strict (MonadState (get, put))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Map (Map)
import GHC.Generics (Generic)
import HaskKV.Log.Class (Entry, LogM (..), TempLogM (..))
import HaskKV.Log.Entry (LogEntry)
import HaskKV.Log.InMem (Log)
import HaskKV.Log.Temp
import HaskKV.Raft.Class (DebugM, PersistM (..), PrintDebugT (..))
import HaskKV.Raft.State
import HaskKV.Server.Types (ServerEvent, ServerM (..), ServerState (sid))
import HaskKV.Snapshot.Types (SnapshotM (..), SnapshotManager)
import HaskKV.Snapshot.Utils (newSnapshotManager)
import HaskKV.Store.Types
import HaskKV.Utils (persistBinary)
import Optics ((^.), use)
import UnliftIO (MonadUnliftIO)
import qualified HaskKV.Server.Instances as Server
import qualified HaskKV.Snapshot.Instances as Snap
import qualified HaskKV.Store.Instances as Store

type SnapshotType = Map

data AppConfig msg k v e = AppConfig
  { cState       :: IORef RaftState
  , cStore       :: Store k v e
  , cTempLog     :: TempLog e
  , cServerState :: ServerState msg
  , cSnapManager :: SnapshotManager
  }

data InitAppConfig msg e = InitAppConfig
  { log           :: Maybe (Log e)
  , state         :: Maybe PersistentState
  , serverState   :: ServerState msg
  , snapDirectory :: Maybe FilePath
  } deriving Generic

instance MonadState RaftState (App msg k v e) where
  get = App $ readIORef . cState
  put x = App $ flip writeIORef x . cState

newtype App msg k v e a = App
  { runApp :: AppConfig msg k v e -> IO a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader (AppConfig msg k v e)
           , MonadUnliftIO ) via ReaderT (AppConfig msg k v e) IO
  deriving DebugM via PrintDebugT (App msg k v e)

instance ServerM msg ServerEvent (App msg k v e) where
  send i msg = asks cServerState >>= liftIO . Server.send i msg
  broadcast msg = asks cServerState >>= liftIO . Server.broadcast msg
  recv = asks cServerState >>= liftIO . Server.recv
  reset e = asks cServerState >>= liftIO . Server.reset e
  serverIds = Server.serverIds <$> asks cServerState

instance (KeyClass k, ValueClass v) => StorageM k v (App msg k v e) where
  getValue k = asks cStore >>= liftIO . Store.getValue k
  {-# INLINABLE getValue #-}
  setValue k v = asks cStore >>= liftIO . Store.setValue k v
  replaceValue k v = asks cStore >>= liftIO . Store.replaceValue k v
  deleteValue k = asks cStore >>= liftIO . Store.deleteValue k
  cleanupExpired t = asks cStore >>= liftIO . Store.cleanupExpired t

instance (Entry e, KeyClass k, ValueClass v) => LogM e (App msg k v e) where
  firstIndex = asks cStore >>= liftIO . Store.firstIndex
  lastIndex = asks cStore >>= liftIO . Store.lastIndex
  loadEntry k = asks cStore >>= liftIO . Store.loadEntry k
  termFromIndex i = asks cStore >>= liftIO . Store.termFromIndex i
  deleteRange a b = asks cStore >>= liftIO . Store.deleteRange a b
  storeEntries es = asks cStore >>= Store.storeEntries es

instance (KeyClass k, ValueClass v)
  => ApplyEntryM k v (LogEntry k v) (App msg k v (LogEntry k v)) where
  applyEntry = Store.applyEntry

instance (KeyClass k, ValueClass v)
   => LoadSnapshotM (Map k v) (App msg k v e) where
  loadSnapshot i t map = asks cStore >>= liftIO . Store.loadSnapshot i t map
  {-# INLINABLE loadSnapshot #-}

instance (Entry e, KeyClass k, ValueClass v)
  => TakeSnapshotM (App msg k v e) where
  takeSnapshot = do
    store <- asks cStore
    lastApplied <- use #lastApplied
    Store.takeSnapshot lastApplied store

instance TempLogM e (App msg k v e) where
  addTemporaryEntry e = asks cTempLog >>= liftIO . addTemporaryEntry' e
  temporaryEntries = asks cTempLog >>= liftIO . temporaryEntries'

instance (KeyClass k, ValueClass v)
  => SnapshotM (SnapshotType k v) (App msg k v e) where
  createSnapshot i t = asks cSnapManager >>= liftIO . Snap.createSnapshot i t
  writeSnapshot o d i = asks cSnapManager >>= liftIO . Snap.writeSnapshot o d i
  saveSnapshot i = asks cSnapManager >>= liftIO . Snap.saveSnapshot i
  readSnapshot i = asks cSnapManager >>= liftIO . Snap.readSnapshot i
  {-# INLINABLE readSnapshot #-}
  hasChunk i = asks cSnapManager >>= liftIO . Snap.hasChunk i
  readChunk a i = asks cSnapManager >>= liftIO . Snap.readChunk a i
  snapshotInfo = asks cSnapManager >>= liftIO . Snap.snapshotInfo

instance PersistM (App msg k v e) where
  persist state = void <$> liftIO $ persistBinary
    persistentStateFilename
    (state ^. #serverID)
    (newPersistentState state)

newAppConfig
  :: (e ~ LogEntry k v) => InitAppConfig msg e -> IO (AppConfig msg k v e)
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
        }
  return config
{-# INLINABLE newAppConfig #-}