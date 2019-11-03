{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Log.Entry where

import Control.Concurrent.STM
import Data.Binary
import Data.Binary.Put
import GHC.Generics
import HaskKV.Log.Class
import HaskKV.Types
import Optics

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

data LogEntryData k v = Change TID k v
                      | Delete TID k
                      | Transaction Transaction
                      | Checkpoint Checkpoint
                      | Noop
                      deriving (Show, Eq, Generic)

data Transaction = Start TID
                 | Commit TID
                 | Abort TID
                 deriving (Show, Eq, Generic)

data Checkpoint = Begin [TID] | End deriving (Show, Eq, Generic)

data LogEntry k v = LogEntry
  { logEntryTerm      :: LogTerm
  , logEntryIndex     :: LogIndex
  , logEntryData      :: LogEntryData k v
  , logEntryCompleted :: Completed
  } deriving (Show, Eq, Generic)
makeFieldLabels ''LogEntry

instance (Show k, Show v, Binary k, Binary v) => Entry (LogEntry k v) where
  entryIndex = (^. #index)
  entryTerm = (^. #term)
  setEntryIndex index = #index .~ index
  setEntryTerm term = #term .~ term

instance (Binary k, Binary v) => Binary (LogEntryData k v)
instance Binary Transaction
instance Binary Checkpoint
instance (Binary k, Binary v) => Binary (LogEntry k v)
