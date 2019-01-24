module HaskKV.Log.Temp where

import Control.Applicative ((<|>))
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Maybe (fromJust)
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Types

import qualified HaskKV.Timer as Timer

newtype TempLog e = TempLog { unTempLog :: MVar [e] }

mkTempLog :: IO (TempLog e)
mkTempLog = TempLog <$> newMVar []

maxTempEntries :: Int
maxTempEntries = 1000

mkTempLogM :: MonadIO m => TempLog e -> TempLogM e m
mkTempLogM log = TempLogM
  { addTemporaryEntry = addTemporaryEntry' log
  , temporaryEntries  = temporaryEntries' log
  }

addTemporaryEntry' :: MonadIO m => TempLog e -> e -> m ()
addTemporaryEntry' (TempLog log) e =
  liftIO $ modifyMVar_ log (pure . addEntry e)
 where
  addEntry e es
    | length es + 1 > maxTempEntries = es
    | otherwise                      = e : es

temporaryEntries' :: MonadIO m => TempLog e -> m [e]
temporaryEntries' (TempLog log) = liftIO $ do
  entries <- reverse <$> takeMVar log
  putMVar log []
  return entries

applyTimeout :: Timeout
applyTimeout = 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
waitApplyEntry
  :: (MonadIO m, HasField "_completed" e Completed)
  => TempLogM e m -> e -> m ()
waitApplyEntry tempLogM entry = do
  addTemporaryEntry tempLogM entry

  liftIO $ do
    timer <- Timer.newIO
    Timer.reset timer applyTimeout
    -- Wait until either something is put in the TMVar
    -- or the timeout is finished.
    atomically $ Timer.await timer <|> awaitCompleted
 where
  awaitCompleted =
    takeTMVar . fromJust . unCompleted . getField @"_completed" $ entry
