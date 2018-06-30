module HaskKV.Types where

import Data.Binary

newtype LogIndex = LogIndex { unLogIndex :: Int }
    deriving (Eq, Num, Ord, Binary)
instance Show LogIndex where
    show (LogIndex i) = show i

newtype LogTerm = LogTerm { unLogTerm :: Int }
    deriving (Eq, Num, Ord, Binary)
instance Show LogTerm where
    show (LogTerm t) = show t

newtype SID = SID { unSID :: Int } deriving (Show, Eq, Num, Binary)
newtype FilePos = FilePos { unFilePos :: Int } deriving (Show, Eq, Num, Binary)
newtype Timeout = Timeout { unTimeout :: Int } deriving (Show, Eq, Num)
newtype Capacity = Capacity { unCapacity :: Int } deriving (Show, Eq)
