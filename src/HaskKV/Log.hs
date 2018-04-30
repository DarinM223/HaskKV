{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Log where

import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary

class (Binary l) => Entry l where
    entryIndex :: l -> Int
    entryTerm  :: l -> Int

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
