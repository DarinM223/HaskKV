{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module HaskKV.Log where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Serialize (Serializable)

class (Serializable l) => Entry l where
    entryIndex :: l -> Int
    entryTerm  :: l -> Int

class (Entry e) => LogM e m where
    -- | Returns the first index written.
    firstIndex :: m Int

    -- | Returns the last index written.
    lastIndex :: m Int

    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m e

    -- | Stores a log entry.
    storeEntry :: e -> m ()

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

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
    deriving (Functor, Applicative, Monad, MonadIO)

instance (Entry e) => LogM e (LogT e m) where
    -- TODO(DarinM223): implement this
