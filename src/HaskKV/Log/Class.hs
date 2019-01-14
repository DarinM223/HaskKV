module HaskKV.Log.Class where

import Data.Binary
import HaskKV.Types

class (Binary l, Show l) => Entry l where
  entryIndex    :: l -> LogIndex
  entryTerm     :: l -> LogTerm
  setEntryIndex :: LogIndex -> l -> l
  setEntryTerm  :: LogTerm -> l -> l

data LogM e m = LogM
  { firstIndex :: m LogIndex
  -- ^ Returns the first index written.
  , lastIndex :: m LogIndex
  -- ^ Returns the last index written.
  , loadEntry :: LogIndex -> m (Maybe e)
  -- ^ Gets a log entry at the specified index.
  , termFromIndex :: LogIndex -> m (Maybe LogTerm)
  -- ^ Gets the term of the entry for the given index.
  , storeEntries :: [e] -> m ()
  -- ^ Stores multiple log entries.
  , deleteRange :: LogIndex -> LogIndex -> m ()
  -- ^ Deletes entries in an inclusive range.
  }

data TempLogM e m = TempLogM
  { addTemporaryEntry :: e -> m ()
  -- ^ Add entry to the temporary entries.
  , temporaryEntries :: m [e]
  -- ^ Removes and returns the temporary entries that haven't
  -- been stored in the log yet.
  }

class HasLogM e m cfg | cfg -> e m where
  getLogM :: cfg -> LogM e m
class HasTempLogM e m cfg | cfg -> e m where
  getTempLogM :: cfg -> TempLogM e m
