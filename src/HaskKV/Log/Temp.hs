module HaskKV.Log.Temp where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import HaskKV.Utils

newtype TempLog e = TempLog { unTempLog :: TVar [e] }

class HasTempLog e c | c -> e where
    getTempLog :: c -> TempLog e

instance HasTempLog e (TempLog e) where
    getTempLog = id

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newTVarIO []

maxTempEntries :: Int
maxTempEntries = 1000

addTemporaryEntryImpl :: (MonadIO m, MonadReader c m, HasTempLog e c)
                      => e
                      -> m ()
addTemporaryEntryImpl e = liftIO
                        . modifyTVarIO (addEntry e)
                        . unTempLog
                      =<< fmap getTempLog ask
  where
    addEntry e es
        | length es + 1 > maxTempEntries = es
        | otherwise                      = e:es

temporaryEntriesImpl :: (MonadIO m, MonadReader c m, HasTempLog e c)
                     => m [e]
temporaryEntriesImpl =
    fmap getTempLog ask >>= \(TempLog var) -> liftIO $ atomically $ do
        entries <- reverse <$> readTVar var
        writeTVar var []
        return entries
