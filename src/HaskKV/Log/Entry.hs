module HaskKV.Log.Entry where

import Control.Concurrent.STM
import Data.Binary
import Data.Binary.Put
import GHC.Generics
import HaskKV.Log

import qualified Data.Binary.Builder as B

newtype TID = TID { unTID :: Int } deriving (Show, Eq, Binary)

newtype Completed = Completed { unCompleted :: Maybe (TMVar ()) }

instance Binary Completed where
    put _ = putBuilder B.empty
    get = return $ Completed Nothing

instance Show Completed where
    show _ = ""

instance Eq Completed where
    (==) _ _ = True

data LogEntry k v = LogEntry
    { _term      :: Int
    , _index     :: Int
    , _data      :: LogEntryData k v
    , _completed :: Completed
    } deriving (Show, Eq, Generic)

data LogEntryData k v = Change TID k v
                      | Delete TID k
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
    setEntryIndex index e = e { _index = index }
    setEntryTerm term e = e { _term = term }

instance (Binary k, Binary v) => Binary (LogEntryData k v)
instance Binary Transaction
instance Binary Checkpoint
instance (Binary k, Binary v) => Binary (LogEntry k v)
