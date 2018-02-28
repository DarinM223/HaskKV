{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module HaskKV.Log where

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
