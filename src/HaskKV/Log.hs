module HaskKV.Log where

import Data.Binary

class (Binary l, Show l) => Entry l where
    entryIndex    :: l -> Int
    entryTerm     :: l -> Int
    setEntryIndex :: Int -> l -> l
    setEntryTerm  :: Int -> l -> l

class (Monad m) => LogM e m | m -> e where
    -- | Returns the first index written.
    firstIndex :: m Int

    -- | Returns the last index written.
    lastIndex :: m Int

    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m (Maybe e)

    -- | Gets the term of the entry for the given index.
    termFromIndex :: Int -> m (Maybe Int)

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

-- | Stores log entries that haven't been
-- stored in the actual log yet.
class (Monad m) => TempLogM e m | m -> e where
    -- | Add entry to the temporary entries.
    addTemporaryEntry :: e -> m ()

    -- | Removes and returns the temporary entries that haven't
    -- been stored in the log yet.
    temporaryEntries :: m [e]
