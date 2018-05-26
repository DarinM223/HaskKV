{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Log where

import Control.Monad.Reader
import Control.Monad.State.Strict
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

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

    default firstIndex :: (MonadTrans t, LogM e m', m ~ t m') => m Int
    default lastIndex :: (MonadTrans t, LogM e m', m ~ t m') => m Int
    default loadEntry :: (MonadTrans t, LogM e m', m ~ t m') => Int -> m (Maybe e)
    default storeEntries :: (MonadTrans t, LogM e m', m ~ t m') => [e] -> m ()
    default deleteRange :: (MonadTrans t, LogM e m', m ~ t m') => Int -> Int -> m ()

    firstIndex = lift firstIndex
    lastIndex = lift lastIndex
    loadEntry = lift . loadEntry
    storeEntries = lift . storeEntries
    deleteRange a b = lift $ deleteRange a b

instance (LogM e m) => LogM e (ReaderT r m)
instance (LogM e m) => LogM e (StateT s m)

-- | Stores log entries that haven't been
-- stored in the actual log yet.
class (Monad m) => TempLogM e m | m -> e where
    -- | Add entry to the temporary entries.
    addTemporaryEntry :: e -> m ()

    -- | Removes and returns the temporary entries that haven't
    -- been stored in the log yet.
    temporaryEntries :: m [e]

    default addTemporaryEntry :: (MonadTrans t, TempLogM e m', m ~ t m') => e -> m ()
    addTemporaryEntry = lift . addTemporaryEntry

    default temporaryEntries :: (MonadTrans t, TempLogM e m', m ~ t m') => m [e]
    temporaryEntries = lift temporaryEntries

instance (TempLogM e m) => TempLogM e (ReaderT r m)
instance (TempLogM e m) => TempLogM e (StateT s m)
