module HaskKV.Types where

import Data.Binary

newtype LogIndex = LogIndex { unLogIndex :: Int }
    deriving (Show, Eq, Num, Ord, Binary)
newtype LogTerm = LogTerm { unLogTerm :: Int }
    deriving (Show, Eq, Num, Ord, Binary)
newtype SID = SID { unSID :: Int } deriving (Show, Eq, Binary)
newtype FilePos = FilePos { unFilePos :: Int } deriving (Show, Eq, Num, Binary)
newtype Timeout = Timeout { unTimeout :: Int } deriving (Show, Eq, Num)
newtype Capacity = Capacity { unCapacity :: Int } deriving (Show, Eq)
