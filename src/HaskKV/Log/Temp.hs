module HaskKV.Log.Temp where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import HaskKV.Utils

newtype TempLog e = TempLog { unTempLog :: TVar [e] }

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newTVarIO []

maxTempEntries :: Int
maxTempEntries = 1000

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
