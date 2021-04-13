module HaskKV.Log.InMem where

import Data.Binary
import Data.Foldable (foldl')
import GHC.Generics
import HaskKV.Log.Class
import HaskKV.Types
import Optics

import qualified Data.IntMap as IM

data Log e = Log
  { entries           :: IM.IntMap e
  , highIdx           :: LogIndex
  , lowIdx            :: LogIndex
  , snapshotLastIndex :: Maybe LogIndex
  , snapshotLastTerm  :: Maybe LogTerm
  } deriving (Show, Generic)

instance (Binary e) => Binary (Log e)

emptyLog :: Log e
emptyLog = Log
  { entries           = IM.empty
  , highIdx           = 0
  , lowIdx            = 0
  , snapshotLastIndex = Nothing
  , snapshotLastTerm  = Nothing
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
  { lowIdx  = lowIndex'
  , highIdx = highIndex'
  , entries = entries'
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
    { entries = entries'
    , lowIdx  = lowIndex
    , highIdx = highIndex
    }
   where
    index     = (entryIndex e)
    entries'  = IM.insert (unLogIndex index) e (l ^. #entries)
    lowIndex  = if l ^. #lowIdx == 0 then index else l ^. #lowIdx
    highIndex = if index > l ^. #highIdx then index else l ^. #highIdx

logFilename :: SID -> FilePath
logFilename (SID sid) = show sid ++ ".log"
