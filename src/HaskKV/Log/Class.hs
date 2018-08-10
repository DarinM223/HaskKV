module HaskKV.Log.Class where

import Data.Binary
import HaskKV.Types

class (Binary l, Show l) => Entry l where
    entryIndex    :: l -> LogIndex
    entryTerm     :: l -> LogTerm
    setEntryIndex :: LogIndex -> l -> l
    setEntryTerm  :: LogTerm -> l -> l

class (Monad m) => LogM e m | m -> e where
    -- | Returns the first index written.
    firstIndex :: m LogIndex

    -- | Returns the last index written.
    lastIndex :: m LogIndex

    -- | Gets a log entry at the specified index.
    loadEntry :: LogIndex -> m (Maybe e)

    -- | Gets the term of the entry for the given index.
    termFromIndex :: LogIndex -> m (Maybe LogTerm)

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    -- | Deletes entries in an inclusive range.
    deleteRange :: LogIndex -> LogIndex -> m ()

-- | Stores log entries that haven't been
-- stored in the actual log yet.
class (Monad m) => TempLogM e m | m -> e where
    -- | Add entry to the temporary entries.
    addTemporaryEntry :: e -> m ()

    -- | Removes and returns the temporary entries that haven't
    -- been stored in the log yet.
    temporaryEntries :: m [e]
