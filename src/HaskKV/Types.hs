module HaskKV.Types where

import Data.Binary

newtype Index = Index { unIndex :: Int } deriving (Show, Eq, Num, Binary)
newtype Term = Term { unTerm :: Int } deriving (Show, Eq, Num, Binary)
newtype SID = SID { unSID :: Int } deriving (Show, Eq, Binary)
newtype FilePos = FilePos { unFilePos :: Int } deriving (Show, Eq, Num, Binary)
newtype Votes = Votes { unVotes :: Int } deriving (Show, Eq, Num, Binary)
