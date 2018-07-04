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
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.Debug
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

isLeader :: IORef RaftState -> IO Bool
isLeader ref = do
    state <- readIORef ref
    case _stateType state of
        Leader _ -> return True
        _        -> return False

data AppConfig msg k v e = AppConfig
    { _state       :: IORef RaftState
    , _store       :: Store k v e
    , _tempLog     :: TempLog e
    , _serverState :: ServerState msg
    , _snapManager :: SnapshotManager
    }

instance HasServerState msg (AppConfig msg k v e) where
    getServerState = _serverState
instance HasStore k v e (AppConfig msg k v e) where
    getStore = _store
instance HasTempLog e (AppConfig msg k v e) where
    getTempLog = _tempLog
instance HasSnapshotManager (AppConfig msg k v e) where
    getSnapshotManager = _snapManager

data InitAppConfig msg e = InitAppConfig
    { _initLog       :: Maybe (Log e)
    , _initState     :: Maybe PersistentState
    , _serverState   :: ServerState msg
    , _snapDirectory :: Maybe FilePath
    }

newAppConfig :: InitAppConfig msg e -> IO (AppConfig msg k v e)
newAppConfig config = do
    let serverState = getField @"_serverState" config
        sid         = getField @"_sid" serverState
        raftState   = newRaftState sid $ _initState config
    raftStateRef <- newIORef raftState
    store <- newStore sid $ _initLog config
    tempLog <- newTempLog
    snapManager <- newSnapshotManager $ _snapDirectory config
    return AppConfig
        { _state       = raftStateRef
        , _store       = store
        , _tempLog     = tempLog
        , _serverState = serverState
        , _snapManager = snapManager
        }

newtype AppT msg k v e a = AppT
    { unAppT :: ReaderT (AppConfig msg k v e) IO a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadReader (AppConfig msg k v e)
             )

type SnapshotType k v = Map k v
instance (Binary k, Binary v) =>
    HasSnapshotType (SnapshotType k v) (AppT msg k v e)

instance MonadState RaftState (AppT msg k v e) where
    get = AppT $ ReaderT $ liftIO . readIORef . _state
    put x = AppT $ ReaderT $ liftIO . flip writeIORef x . _state

$(deriveVia [t| forall msg k v e. ServerM msg ServerEvent (AppT msg k v e)
                            `Via` ServerT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (KeyClass k, ValueClass v, Entry e) =>
                StorageM k v (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v. (KeyClass k, ValueClass v) =>
                ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v))
          `Via` StoreT (AppT msg k v (LogEntry k v)) |])
$(deriveVia [t| forall msg k v e. (Entry e, KeyClass k, ValueClass v) =>
                LogM e (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (KeyClass k, ValueClass v, Entry e) =>
                LoadSnapshotM (SnapshotType k v) (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (Entry e, KeyClass k, ValueClass v) =>
                TakeSnapshotM (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. TempLogM e (AppT msg k v e)
                            `Via` TempLogT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (Binary k, Binary v) =>
                SnapshotM (SnapshotType k v) (AppT msg k v e)
          `Via` SnapshotT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. DebugM (AppT msg k v e)
                            `Via` PrintDebugT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. PersistM (AppT msg k v e)
                            `Via` PersistT (AppT msg k v e) |])

runAppT :: AppT msg k v e a -> AppConfig msg k v e -> IO a
runAppT m config = flip runReaderT config . unAppT $ m
