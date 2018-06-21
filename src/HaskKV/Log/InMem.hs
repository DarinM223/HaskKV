module HaskKV.Log.InMem where

import Data.List
import HaskKV.Log
import qualified Data.IntMap as IM

data Log e = Log
    { _entries           :: IM.IntMap e
    , _highIdx           :: Int
    , _lowIdx            :: Int
    , _snapshotLastIndex :: Maybe Int
    } deriving (Show)

emptyLog :: Log e
emptyLog = Log { _entries           = IM.empty
               , _highIdx           = 0
               , _lowIdx            = 0
               , _snapshotLastIndex = Nothing
               }

lastIndexLog :: Log e -> Int
lastIndexLog l = case _snapshotLastIndex l of
    Just index | index > _highIdx l -> index
    _                               -> _highIdx l

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
