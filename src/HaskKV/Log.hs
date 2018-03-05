module HaskKV.Log where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Serialize (Serializable)
import HaskKV.Store

class (Serializable l) => Entry l where
    entryIndex :: l -> Int
    entryTerm  :: l -> Int

class (Monad m) => LogM m where
    -- | Returns the first index written.
    firstIndex :: m Int

    -- | Returns the last index written.
    lastIndex :: m Int

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

    default firstIndex :: (MonadTrans t, LogM m', m ~ t m') => m Int
    firstIndex = lift firstIndex

    default lastIndex :: (MonadTrans t, LogM m', m ~ t m') => m Int
    lastIndex = lift lastIndex

    default deleteRange :: (MonadTrans t, LogM m', m ~ t m')
                        => Int
                        -> Int
                        -> m ()
    deleteRange a b = lift $ deleteRange a b

class (LogM m, Entry e) => LogME e m where
    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m e

    -- | Stores a log entry.
    storeEntry :: e -> m ()

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    default loadEntry :: (MonadTrans t, LogME e m', m ~ t m') => Int -> m e
    loadEntry = lift . loadEntry

    default storeEntry :: (MonadTrans t, LogME e m', m ~ t m') => e -> m ()
    storeEntry = lift . storeEntry

    default storeEntries :: (MonadTrans t, LogME e m', m ~ t m') => [e] -> m ()
    storeEntries = lift . storeEntries

data LogEntry = LogEntry
    { _term :: Int
    , _data :: LogEntryData
    } deriving (Show, Eq)

data LogEntryData
    = Insert
    | Change
    deriving (Show, Eq)

instance Entry LogEntry where
    -- TODO(DarinM223): implement this

instance Serializable LogEntry where
    -- TODO(DarinM223): implement this

data Log = Log
    { _entries :: [LogEntry]
    }

newtype LogT e m a = LogT { unLogT :: ReaderT (TVar Log) m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadTrans)

instance (Monad m) => LogM (LogT e m) where
    -- TODO(DarinM223): implement this
    firstIndex = undefined
    lastIndex = undefined
    deleteRange = undefined

instance (Monad m, Entry e) => LogME e (LogT e m) where
    -- TODO(DarinM223): implement this
    loadEntry = undefined
    storeEntry = undefined
    storeEntries = undefined

instance (StorageM m) => StorageM (LogT e m) where
instance (StorageMK k m) => StorageMK k (LogT e m) where
instance (StorageMKV k v m) => StorageMKV k v (LogT e m) where
instance (LogM m) => LogM (ReaderT r m) where
instance (LogME e m) => LogME e (ReaderT r m) where
