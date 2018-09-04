{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Monad where

import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Binary.Orphans ()
import Data.Deriving.Via
import Data.IORef
import GHC.Records
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
  { _state       :: IORef RaftState
  , _store       :: Store k v e
  , _tempLog     :: TempLog e
  , _serverState :: ServerState msg
  , _snapManager :: SnapshotManager
  , _run         :: Fn msg k v e
  }

instance HasServerState msg (AppConfig msg k v e) where
  getServerState = _serverState
instance HasStore k v e (AppConfig msg k v e) where
  getStore = _store
instance HasTempLog e (AppConfig msg k v e) where
  getTempLog = _tempLog
instance HasSnapshotManager (AppConfig msg k v e) where
  getSnapshotManager = _snapManager
instance HasRun msg k v e (AppConfig msg k v e) where
  getRun = _run

data InitAppConfig msg e = InitAppConfig
  { _initLog       :: Maybe (Log e)
  , _initState     :: Maybe PersistentState
  , _serverState   :: ServerState msg
  , _snapDirectory :: Maybe FilePath
  }

newtype App msg k v e a = App
  { unApp :: ReaderT (AppConfig msg k v e) IO a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader (AppConfig msg k v e)
           )

instance (Binary k, Binary v) =>
  HasSnapshotType (SnapshotType k v) (App msg k v e)

instance MonadState RaftState (App msg k v e) where
  get = App $ ReaderT $ liftIO . readIORef . _state
  put x = App $ ReaderT $ liftIO . flip writeIORef x . _state

$(deriveVia [t| forall msg k v e.
                ServerM msg ServerEvent (App msg k v e)
          `Via` ServerT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e) =>
                StorageM k v (App msg k v e)
          `Via` StoreT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e, e ~ LogEntry k v) =>
                ApplyEntryM k v e (App msg k v e)
          `Via` StoreT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e) =>
                LogM e (App msg k v e)
          `Via` StoreT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e) =>
                LoadSnapshotM (SnapshotType k v) (App msg k v e)
          `Via` StoreT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e) =>
                TakeSnapshotM (App msg k v e)
          `Via` StoreT (App msg k v e) |])
$(deriveVia [t| forall msg k v e.
                TempLogM e (App msg k v e)
          `Via` TempLogT (App msg k v e) |])
$(deriveVia [t| forall msg k v e. (Constr k v e) =>
                SnapshotM (SnapshotType k v) (App msg k v e)
          `Via` SnapshotT (App msg k v e) |])
$(deriveVia [t| forall msg k v e.
                DebugM (App msg k v e)
          `Via` PrintDebugT (App msg k v e) |])
$(deriveVia [t| forall msg k v e.
                PersistM (App msg k v e)
          `Via` PersistT (App msg k v e) |])

runApp :: App msg k v e a -> AppConfig msg k v e -> IO a
runApp m config = flip runReaderT config . unApp $ m

newAppConfig
  :: (KeyClass k, ValueClass v, e ~ LogEntry k v)
  => InitAppConfig msg e
  -> IO (AppConfig msg k v e)
newAppConfig config = do
  let
    serverState = getField @"_serverState" config
    sid         = getField @"_sid" serverState
    raftState   = newRaftState sid $ _initState config
  raftStateRef <- newIORef raftState
  store        <- newStore sid $ _initLog config
  tempLog      <- newTempLog
  snapManager  <- newSnapshotManager $ _snapDirectory config
  let
    config = AppConfig
      { _state       = raftStateRef
      , _store       = store
      , _tempLog     = tempLog
      , _serverState = serverState
      , _snapManager = snapManager
      , _run         = flip runApp config
      }
  return config
