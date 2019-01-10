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
class HasTempLog e r | r -> e where getTempLog :: r -> TempLog e

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newMVar []

maxTempEntries :: Int
maxTempEntries = 1000

type TLClass e r m = (HasTempLog e r, MonadReader r m, MonadIO m)

tempLogMIO :: TLClass e r m => TempLogM e m
tempLogMIO = TempLogM
  { addTemporaryEntry = addTemporaryEntry'
  , temporaryEntries  = temporaryEntries'
  }

getTL :: TLClass e r m => (TempLog e -> IO a) -> m a
getTL m = asks getTempLog >>= liftIO . m

addTemporaryEntry' :: TLClass e r m => e -> m ()
addTemporaryEntry' e = getTL $ flip modifyMVar_ (pure . addEntry e) . unTempLog
 where
  addEntry e es
    | length es + 1 > maxTempEntries = es
    | otherwise                      = e : es

temporaryEntries' :: TLClass e r m => m [e]
temporaryEntries' = getTL $ \(TempLog var) -> do
  entries <- reverse <$> takeMVar var
  putMVar var []
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
