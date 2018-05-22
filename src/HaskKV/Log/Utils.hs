module HaskKV.Log.Utils where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Maybe (fromJust)
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry

import qualified HaskKV.Timer as Timer

type Index      = Int
type StartIndex = Int
type EndIndex   = Int
type LastIndex  = Int

-- | Returns the previous log index.
prevIndex :: Index -> Index
prevIndex index = if index <= 0 then 0 else index - 1

-- | Returns a range of entries from start to end inclusive.
entryRange :: (LogM e m) => StartIndex -> EndIndex -> m (Maybe [e])
entryRange = getEntries []
  where
    getEntries entries start end
        | end < start = return $ Just entries
        | otherwise = do
            entry <- loadEntry end
            case entry of
                Just e  -> getEntries (e:entries) start (prevIndex end)
                Nothing -> return Nothing

-- | Clears all entries from the log that are different from the entries
-- passed in.
--
-- Returns a subset of the passed in entries with only the entries that
-- don't exist in the log.
diffEntriesWithLog :: (LogM e m, Entry e)
                   => LastIndex -- Index of the last entry in the log.
                   -> [e]       -- Entries to append.
                   -> m [e]
diffEntriesWithLog last entries = do
    entriesStart <- findStart last (zip [0..] entries)
    case entriesStart of
        Just start -> return $ drop start entries
        Nothing    -> return []
  where
    -- Returns the first index in the entries that doesn't exist in the log
    -- or is different from the existing entry in the log.
    findStart _ [] = return Nothing
    findStart lastIndex ((i, e):es)
        | entryIndex e > lastIndex = return $ Just i
        | otherwise = do
            storeEntry <- loadEntry $ entryIndex e
            case storeEntry of
                Just se | entryTerm e /= entryTerm se -> do
                    deleteRange (entryIndex e) lastIndex
                    return $ Just i
                _ -> findStart lastIndex es

applyTimeout :: Timer.Timeout
applyTimeout = Timer.Timeout 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
apply :: (MonadIO m, LogM e m, HasField "_completed" e Completed) => e -> m ()
apply entry = do
    storeEntries [entry]

    timer <- liftIO $ Timer.newIO
    Timer.reset timer applyTimeout
    -- Wait until either something is put in the TMVar
    -- or the timeout is finished.
    liftIO . atomically $ (Timer.await timer) `orElse` awaitCompleted
  where
    awaitCompleted = takeTMVar
                   . fromJust
                   . unCompleted
                   . getField @"_completed"
                   $ entry
