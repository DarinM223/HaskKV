module HaskKV.Monad where

import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State.Strict
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Store

data AppConfig msg k v e = AppConfig
    { _store       :: TVar (Store k v e)
    , _serverState :: ServerState msg
    , _isLeader    :: TVar Bool
    }

newtype AppT msg k v e a = AppT
    { unAppT :: StateT RaftState (ReaderT (AppConfig msg k v e) IO) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadState RaftState
             , MonadReader (AppConfig msg k v e)
             )

runAppT m config raftState = flip runReaderT config
                           . flip runStateT raftState
                           . unAppT
                           $ m

runAppTConfig m config = flip runReaderT config
                       . fmap fst
                       . flip runStateT emptyState
                       . unAppT
                       $ m
  where
    emptyState = createRaftState 0

instance ServerM msg ServerEvent (AppT msg k v e) where
    send i msg = asks _serverState >>= sendImpl i msg
    broadcast msg = asks _serverState >>= broadcastImpl msg
    recv = asks _serverState >>= recvImpl
    inject e = asks _serverState >>= injectImpl e
    reset e = asks _serverState >>= resetImpl e
    serverIds = serverIdsImpl <$> asks _serverState

instance (KeyClass k, ValueClass v) => StorageM k v (AppT msg k v e) where
    getValue k = asks _store >>= getValueImpl k
    setValue k v = asks _store >>= setValueImpl k v
    replaceValue k v = asks _store >>= replaceValueImpl k v
    deleteValue k = asks _store >>= deleteValueImpl k
    cleanupExpired t = asks _store >>= cleanupExpiredImpl t

instance (Entry e) => LogM e (AppT msg k v e) where
    firstIndex = asks _store >>= firstIndexImpl
    lastIndex = asks _store >>= lastIndexImpl
    loadEntry k = asks _store >>= loadEntryImpl k
    storeEntries es = asks _store >>= storeEntriesImpl es
    deleteRange a b = asks _store >>= deleteRangeImpl a b

instance TempLogM e (AppT msg k v e) where
    addTemporaryEntry e = asks _store >>= addTemporaryEntryImpl e
    temporaryEntries = asks _store >>= temporaryEntriesImpl

instance (KeyClass k, ValueClass v) =>
    ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v)) where

    applyEntry = applyEntryImpl
