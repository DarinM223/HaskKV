module HaskKV.Monad where

import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State.Strict
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.Temp
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

data AppConfig msg k v e = AppConfig
    { _store       :: TVar (Store k v e)
    , _tempLog     :: TempLog e
    , _serverState :: ServerState msg
    , _isLeader    :: TVar Bool
    , _snapshots   :: TVar SnapshotManager
    }

newAppConfig :: ServerState msg -> IO (AppConfig msg k v e)
newAppConfig serverState = do
    isLeader <- newTVarIO False
    store <- newTVarIO emptyStore
    tempLog <- newTempLog
    snapshots <- newSnapshotManager
    return AppConfig
        { _store       = store
        , _tempLog     = tempLog
        , _serverState = serverState
        , _isLeader    = isLeader
        , _snapshots   = snapshots
        }

newtype AppT msg k v e a = AppT
    { unAppT :: StateT RaftState (ReaderT (AppConfig msg k v e) IO) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadState RaftState
             , MonadReader (AppConfig msg k v e)
             )

runAppT :: AppT msg k v e a
        -> AppConfig msg k v e
        -> RaftState
        -> IO (a, RaftState)
runAppT m config raftState = flip runReaderT config
                           . flip runStateT raftState
                           . unAppT
                           $ m

runAppTConfig :: AppT msg k v e a -> AppConfig msg k v e -> IO a
runAppTConfig m config = flip runReaderT config
                       . fmap fst
                       . flip runStateT emptyState
                       . unAppT
                       $ m
  where
    emptyState = newRaftState 0

instance HasServerState msg (AppConfig msg k v e) where
    getServerState = _serverState
instance HasStore k v e (AppConfig msg k v e) where
    getStore = _store
instance HasTempLog e (AppConfig msg k v e) where
    getTempLog = _tempLog
instance HasSnapshotManager (AppConfig msg k v e) where
    getSnapshotManager = _snapshots

instance ServerM msg ServerEvent (AppT msg k v e) where
    send      = sendImpl
    broadcast = broadcastImpl
    recv      = recvImpl
    inject    = injectImpl
    reset     = resetImpl
    serverIds = serverIdsImpl

instance (KeyClass k, ValueClass v) => StorageM k v (AppT msg k v e) where
    getValue       = getValueImpl
    setValue       = setValueImpl
    replaceValue   = replaceValueImpl
    deleteValue    = deleteValueImpl
    cleanupExpired = cleanupExpiredImpl

instance (Entry e) => LogM e (AppT msg k v e) where
    firstIndex   = firstIndexImpl
    lastIndex    = lastIndexImpl
    loadEntry    = loadEntryImpl
    storeEntries = storeEntriesImpl
    deleteRange  = deleteRangeImpl

instance TempLogM e (AppT msg k v e) where
    addTemporaryEntry = addTemporaryEntryImpl
    temporaryEntries  = temporaryEntriesImpl

instance (KeyClass k, ValueClass v) =>
    ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v)) where

    applyEntry = applyEntryImpl

instance SnapshotM (AppT msg k v (LogEntry k v)) where
    createSnapshot = createSnapshotImpl
    writeSnapshot  = writeSnapshotImpl
    saveSnapshot   = saveSnapshotImpl
