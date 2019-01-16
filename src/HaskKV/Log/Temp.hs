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

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newMVar []

maxTempEntries :: Int
maxTempEntries = 1000

class HasTempLog e cfg | cfg -> e where
  getTempLog :: cfg -> TempLog e

type TLClass e cfg m = (HasTempLog e cfg, MonadIO m)

mkTempLogM :: TLClass e cfg m => cfg -> TempLogM e m
mkTempLogM cfg = TempLogM
  { addTemporaryEntry = addTemporaryEntry' cfg
  , temporaryEntries  = temporaryEntries' cfg
  }

addTemporaryEntry' :: TLClass e cfg m => cfg -> e -> m ()
addTemporaryEntry' cfg e =
  liftIO . flip modifyMVar_ (pure . addEntry e) . unTempLog . getTempLog $ cfg
 where
  addEntry e es
    | length es + 1 > maxTempEntries = es
    | otherwise                      = e : es

temporaryEntries' :: TLClass e cfg m => cfg -> m [e]
temporaryEntries' cfg = liftIO $ do
  let (TempLog var) = getTempLog cfg
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
