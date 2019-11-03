{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Log.InMem where

import Data.Binary
import Data.Foldable (foldl')
import GHC.Generics
import HaskKV.Log.Class
import HaskKV.Types
import Optics

import qualified Data.IntMap as IM

data Log e = Log
  { logEntries           :: IM.IntMap e
  , logHighIdx           :: LogIndex
  , logLowIdx            :: LogIndex
  , logSnapshotLastIndex :: Maybe LogIndex
  , logSnapshotLastTerm  :: Maybe LogTerm
  } deriving (Show, Generic)
makeFieldLabels ''Log

instance (Binary e) => Binary (Log e)

emptyLog :: Log e
emptyLog = Log
  { logEntries           = IM.empty
  , logHighIdx           = 0
  , logLowIdx            = 0
  , logSnapshotLastIndex = Nothing
  , logSnapshotLastTerm  = Nothing
  }

lastIndexLog :: Log e -> LogIndex
lastIndexLog l = case l ^. #snapshotLastIndex of
  Just index | index > l ^. #highIdx -> index
  _                                  -> l ^. #highIdx

entryTermLog :: (Entry e) => LogIndex -> Log e -> Maybe LogTerm
entryTermLog i log
  | i <= 0 = Just 0
  | log ^. #snapshotLastIndex == Just i = log ^. #snapshotLastTerm
  | otherwise = fmap entryTerm . IM.lookup (unLogIndex i) $ log ^. #entries

deleteRangeLog :: LogIndex -> LogIndex -> Log e -> Log e
deleteRangeLog min max l = l
  { logLowIdx  = lowIndex'
  , logHighIdx = highIndex'
  , logEntries = entries'
  }
 where
  indexRange min max = [unLogIndex min .. unLogIndex max]
  entries'  = foldl' (flip IM.delete) (l ^. #entries) $ indexRange min max
  lowIndex  = if min <= l ^. #lowIdx then max + 1 else l ^. #lowIdx
  highIndex = if max >= l ^. #highIdx then min - 1 else l ^. #highIdx
  (lowIndex', highIndex') =
    if lowIndex > highIndex then (0, 0) else (lowIndex, highIndex)

storeEntriesLog :: (Entry e) => [e] -> Log e -> Log e
storeEntriesLog es l = foldl' addEntry l es
 where
  addEntry l e = l
    { logEntries = entries'
    , logLowIdx  = lowIndex
    , logHighIdx = highIndex
    }
   where
    index     = (entryIndex e)
    entries'  = IM.insert (unLogIndex index) e (l ^. #entries)
    lowIndex  = if l ^. #lowIdx == 0 then index else l ^. #lowIdx
    highIndex = if index > l ^. #highIdx then index else l ^. #highIdx

logFilename :: SID -> FilePath
logFilename (SID sid) = show sid ++ ".log"
