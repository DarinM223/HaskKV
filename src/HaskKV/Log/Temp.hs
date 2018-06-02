module HaskKV.Log.Temp where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Maybe (fromJust)
import GHC.Records
import HaskKV.Utils
import HaskKV.Log.Entry

import qualified HaskKV.Timer as Timer

newtype TempLog e = TempLog { unTempLog :: TVar [e] }

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newTVarIO []

maxTempEntries :: Int
maxTempEntries = 1000

-- TempLogM instance implementations.

addTemporaryEntryImpl :: (MonadIO m) => e -> TempLog e -> m ()
addTemporaryEntryImpl e = liftIO . modifyTVarIO (addEntry e) . unTempLog
  where
    addEntry e es
        | length es + 1 > maxTempEntries = es
        | otherwise                      = e:es

temporaryEntriesImpl :: (MonadIO m) => TempLog e -> m [e]
temporaryEntriesImpl (TempLog var) =
    liftIO $ atomically $ do
        entries <- reverse <$> readTVar var
        writeTVar var []
        return entries

applyTimeout :: Timer.Timeout
applyTimeout = Timer.Timeout 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
waitApplyEntry :: (MonadIO m, HasField "_completed" e Completed)
               => e
               -> TempLog e
               -> m ()
waitApplyEntry entry temp = liftIO $ do
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
