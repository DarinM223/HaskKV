{-# LANGUAGE LambdaCase #-}
module HaskKV.Log.Utils where

import HaskKV.Log.Class
import HaskKV.Types (LogIndex)

type Index      = LogIndex
type StartIndex = Index
type EndIndex   = Index
type LastIndex  = Index

-- | Returns the previous log index.
prevIndex :: Index -> Index
prevIndex index = if index <= 0 then 0 else index - 1

-- | Returns a range of entries from start to end inclusive.
entryRange :: (LogM e m) => StartIndex -> EndIndex -> m (Maybe [e])
entryRange = getEntries []
 where
  getEntries entries start end
    | end < start = return $ Just entries
    | otherwise = loadEntry end >>= \case
      Just entry -> getEntries (entry : entries) start (prevIndex end)
      Nothing    -> return Nothing

-- | Clears all entries from the log that are different from the entries
-- passed in.
--
-- Returns a subset of the passed in entries with only the entries that
-- don't exist in the log.
diffEntriesWithLog
  :: (LogM e m, Entry e)
  => LastIndex -- Index of the last entry in the log.
  -> [e]       -- Entries to append.
  -> m [e]
diffEntriesWithLog last entries =
  diffEntriesStart last (zip [0 ..] entries) >>= pure . \case
    Just start -> drop start entries
    Nothing    -> []
 where
  -- Returns the first index in the entries that doesn't exist in the log
  -- or is different from the existing entry in the log.
  diffEntriesStart _ [] = return Nothing
  diffEntriesStart lastIndex ((i, e) : es)
    | entryIndex e > lastIndex = return $ Just i
    | otherwise = termFromIndex (entryIndex e) >>= \case
      Just term | entryTerm e /= term -> do
        deleteRange (entryIndex e) lastIndex
        return $ Just i
      _ -> diffEntriesStart lastIndex es
