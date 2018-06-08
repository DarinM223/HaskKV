{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Log.Temp where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.Maybe (fromJust)
import GHC.Records
import HaskKV.Utils
import HaskKV.Log
import HaskKV.Log.Entry

import qualified HaskKV.Timer as Timer

newtype TempLog e = TempLog { unTempLog :: TVar [e] }

class HasTempLog e r | r -> e where
    getTempLog :: r -> TempLog e

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newTVarIO []

maxTempEntries :: Int
maxTempEntries = 1000

newtype TempLogT m a = TempLogT { unTempLogT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance
    ( MonadIO m
    , MonadReader r m
    , HasTempLog e r
    ) => TempLogM e (TempLogT m) where

    addTemporaryEntry e = liftIO . addTemporaryEntryImpl e =<< asks getTempLog
    temporaryEntries = liftIO . temporaryEntriesImpl =<< asks getTempLog

addTemporaryEntryImpl :: e -> TempLog e -> IO ()
addTemporaryEntryImpl e = modifyTVarIO (addEntry e) . unTempLog
  where
    addEntry e es
        | length es + 1 > maxTempEntries = es
        | otherwise                      = e:es

temporaryEntriesImpl :: TempLog e -> IO [e]
temporaryEntriesImpl (TempLog var) = atomically $ do
    entries <- reverse <$> readTVar var
    writeTVar var []
    return entries

applyTimeout :: Timer.Timeout
applyTimeout = Timer.Timeout 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
waitApplyEntry :: (HasField "_completed" e Completed) => e -> TempLog e -> IO ()
waitApplyEntry entry temp = do
    addTemporaryEntryImpl entry temp

    timer <- Timer.newIO
    Timer.reset timer applyTimeout
    -- Wait until either something is put in the TMVar
    -- or the timeout is finished.
    atomically $ (Timer.await timer) `orElse` awaitCompleted
  where
    awaitCompleted = takeTMVar
                   . fromJust
                   . unCompleted
                   . getField @"_completed"
                   $ entry
