{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}

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

class (LogM m, Entry e) => LogME e m where
    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m e

    -- | Stores a log entry.
    storeEntry :: e -> m ()

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

data LogEntry = LogEntry
    { leTerm :: Int
    , leData :: LogEntryData
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
    { entries :: [LogEntry]
    }

newtype LogT e m a = LogT { unLogT :: ReaderT (TVar Log) m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadTrans)

instance (Monad m) => LogM (LogT e m) where
    -- TODO(DarinM223): implement this

instance (Monad m, Entry e) => LogME e (LogT e m) where
    -- TODO(DarinM223): implement this

instance (StorageM m) => StorageM (LogT e m) where
    cleanupExpired = lift . cleanupExpired

instance (StorageMK k m) => StorageMK k (LogT e m) where
    deleteValue = lift . deleteValue

instance (StorageMKV k v m) => StorageMKV k v (LogT e m) where
    getValue = lift . getValue
    setValue k v = lift $ setValue k v
    replaceValue k v = lift $ replaceValue k v

instance (LogM m) => LogM (ReaderT r m) where
    firstIndex = lift firstIndex
    lastIndex = lift lastIndex
    deleteRange k v = lift $ deleteRange k v

instance (LogME e m) => LogME e (ReaderT r m) where
    loadEntry = lift . loadEntry
    storeEntry = lift . storeEntry
    storeEntries = lift . storeEntries
