{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Log.Temp where

import Control.Applicative ((<|>))
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.Maybe (fromJust)
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Types

import qualified HaskKV.Timer as Timer

newtype TempLog e = TempLog { unTempLog :: MVar [e] }

class HasTempLog e r | r -> e where
    getTempLog :: r -> TempLog e

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newMVar []

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
addTemporaryEntryImpl e = flip modifyMVar_ (pure . addEntry e) . unTempLog
  where
    addEntry e es
        | length es + 1 > maxTempEntries = es
        | otherwise                      = e:es

temporaryEntriesImpl :: TempLog e -> IO [e]
temporaryEntriesImpl (TempLog var) = do
    entries <- reverse <$> takeMVar var
    putMVar var []
    return entries

applyTimeout :: Timeout
applyTimeout = 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
waitApplyEntry :: (MonadIO m, TempLogM e m, HasField "_completed" e Completed)
               => e
               -> m ()
waitApplyEntry entry = do
    addTemporaryEntry entry

    liftIO $ do
        timer <- Timer.newIO
        Timer.reset timer applyTimeout
        -- Wait until either something is put in the TMVar
        -- or the timeout is finished.
        atomically $ Timer.await timer <|> awaitCompleted
  where
    awaitCompleted = takeTMVar
                   . fromJust
                   . unCompleted
                   . getField @"_completed"
                   $ entry
