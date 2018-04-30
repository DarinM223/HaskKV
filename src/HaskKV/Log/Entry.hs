module HaskKV.Log.Entry where

import Data.Binary
import GHC.Generics
import HaskKV.Log

newtype TID = TID { unTID :: Int } deriving (Show, Eq, Binary)

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
