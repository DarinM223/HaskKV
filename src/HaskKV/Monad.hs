{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Monad where

import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Binary.Orphans ()
import Data.Deriving.Via
import Data.IORef
import Data.Map (Map)
import GHC.Records
import HaskKV.Constr
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.Debug
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

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

newtype AppT msg k v e m a = AppT
    { unAppT :: ReaderT (AppConfig msg k v e) m a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader (AppConfig msg k v e)
             )

type SnapshotType k v = Map k v
instance (Binary k, Binary v) =>
    HasSnapshotType (SnapshotType k v) (AppT msg k v e m)

instance (MonadIO m) => MonadState RaftState (AppT msg k v e m) where
    get = AppT $ ReaderT $ liftIO . readIORef . _state
    put x = AppT $ ReaderT $ liftIO . flip writeIORef x . _state

instance MonadTrans (AppT msg k v e) where
    lift = AppT . lift

$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                ServerM msg ServerEvent (AppT msg k v e m)
          `Via` ServerT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                StorageM k v (AppT msg k v e m)
          `Via` StoreT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v m. (Constr k v (LogEntry k v) m) =>
                ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v) m)
          `Via` StoreT (AppT msg k v (LogEntry k v) m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                LogM e (AppT msg k v e m)
          `Via` StoreT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                LoadSnapshotM (SnapshotType k v) (AppT msg k v e m)
          `Via` StoreT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                TakeSnapshotM (AppT msg k v e m)
          `Via` StoreT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                TempLogM e (AppT msg k v e m)
          `Via` TempLogT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                SnapshotM (SnapshotType k v) (AppT msg k v e m)
          `Via` SnapshotT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                DebugM (AppT msg k v e m)
          `Via` PrintDebugT (AppT msg k v e m) |])
$(deriveVia [t| forall msg k v e m. (Constr k v e m) =>
                PersistM (AppT msg k v e m)
          `Via` PersistT (AppT msg k v e m) |])

runAppT :: AppT msg k v e m a -> AppConfig msg k v e -> m a
runAppT m config = flip runReaderT config . unAppT $ m

newAppConfig :: (KeyClass k, ValueClass v, Entry e) => InitAppConfig msg e -> IO (AppConfig msg k v e)
newAppConfig config = do
    let serverState = getField @"_serverState" config
        sid         = getField @"_sid" serverState
        raftState   = newRaftState sid $ _initState config
    raftStateRef <- newIORef raftState
    store <- newStore sid $ _initLog config
    tempLog <- newTempLog
    snapManager <- newSnapshotManager $ _snapDirectory config
    let config = AppConfig
            { _state       = raftStateRef
            , _store       = store
            , _tempLog     = tempLog
            , _serverState = serverState
            , _snapManager = snapManager
            , _run         = flip runAppT config
            }
    return config
