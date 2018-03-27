{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Log where

import Control.Monad.Reader
import Data.Binary
import Data.List
import GHC.Generics

import qualified Data.IntMap as IM

class (Binary l) => Entry l where
    entryIndex :: l -> Int
    entryTerm  :: l -> Int

class (Monad m) => LogM e m | m -> e where
    -- | Returns the first index written.
    firstIndex :: m Int

    -- | Returns the last index written.
    lastIndex :: m Int

    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m (Maybe e)

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

    default firstIndex :: (MonadTrans t, LogM e m', m ~ t m') => m Int
    default lastIndex :: (MonadTrans t, LogM e m', m ~ t m') => m Int
    default loadEntry :: (MonadTrans t, LogM e m', m ~ t m') => Int -> m (Maybe e)
    default storeEntries :: (MonadTrans t, LogM e m', m ~ t m') => [e] -> m ()
    default deleteRange :: (MonadTrans t, LogM e m', m ~ t m') => Int -> Int -> m ()

    firstIndex = lift firstIndex
    lastIndex = lift lastIndex
    loadEntry = lift . loadEntry
    storeEntries = lift . storeEntries
    deleteRange a b = lift $ deleteRange a b

instance (LogM e m) => LogM e (ReaderT r m)

type TID = Int

data LogEntry k v = LogEntry
    { _term  :: Int
    , _index :: Int
    , _data  :: LogEntryData k v
    } deriving (Show, Eq, Generic)

data LogEntryData k v = Insert TID k
                      | Change TID k v
                      | Transaction Transaction
                      | Checkpoint Checkpoint
                      deriving (Show, Eq, Generic)

data Transaction = Start TID
                 | Commit TID
                 | Abort TID
                 deriving (Show, Eq, Generic)

data Checkpoint = Begin [TID] | End deriving (Show, Eq, Generic)

instance (Binary k, Binary v) => Entry (LogEntry k v) where
    entryIndex = _index
    entryTerm = _term

instance (Binary k, Binary v) => Binary (LogEntryData k v)
instance Binary Transaction
instance Binary Checkpoint
instance (Binary k, Binary v) => Binary (LogEntry k v)

data Log e = Log
    { _entries :: IM.IntMap e
    , _highIdx :: Int
    , _lowIdx  :: Int
    } deriving (Show)

emptyLog :: Log e
emptyLog = Log { _entries = IM.empty, _highIdx = 0, _lowIdx = 0 }

deleteRangeLog :: Int -> Int -> Log e -> Log e
deleteRangeLog min max l =
    l { _lowIdx = lowIndex', _highIdx = highIndex', _entries = entries' }
  where
    entries' = foldl' (flip IM.delete) (_entries l) [min..max]
    lowIndex = if min <= _lowIdx l then max + 1 else _lowIdx l
    highIndex = if max >= _highIdx l then min - 1 else _highIdx l
    (lowIndex', highIndex') = if lowIndex > highIndex
        then (0, 0)
        else (lowIndex, highIndex)

storeEntriesLog :: (Entry e) => [e] -> Log e -> Log e
storeEntriesLog es l = foldl' addEntry l es
  where
    addEntry l e =
        l { _entries = entries', _lowIdx = lowIndex, _highIdx = highIndex }
      where
        index = (entryIndex e)
        entries' = IM.insert index e (_entries l)
        lowIndex = if _lowIdx l == 0 then index else _lowIdx l
        highIndex = if index > _highIdx l then index else _highIdx l
