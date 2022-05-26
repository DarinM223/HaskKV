{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Log.Temp where

import Control.Applicative ((<|>))
import Control.Concurrent.MVar (MVar, modifyMVar_, newMVar, putMVar, takeMVar)
import Control.Concurrent.STM (atomically, takeTMVar)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader)
import Data.Maybe (fromJust)
import HaskKV.Log.Class (TempLogM (..))
import HaskKV.Log.Entry (Completed (unCompleted))
import HaskKV.Types (Timeout)
import Optics ((^.), A_Lens, LabelOptic', Lens', ViewableOptic (gview))

import qualified HaskKV.Timer as Timer

newtype TempLog e = TempLog { unTempLog :: MVar [e] }

class HasTempLog e r | r -> e where
  tempLogL :: Lens' r (TempLog e)

newTempLog :: IO (TempLog e)
newTempLog = TempLog <$> newMVar []

maxTempEntries :: Int
maxTempEntries = 1000

newtype TempLogT m a = TempLogT { unTempLogT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance (MonadIO m, MonadReader r m, HasTempLog e r)
  => TempLogM e (TempLogT m) where

  addTemporaryEntry e = gview tempLogL >>= liftIO . addTemporaryEntry' e
  temporaryEntries = gview tempLogL >>= liftIO . temporaryEntries'

addTemporaryEntry' :: e -> TempLog e -> IO ()
addTemporaryEntry' e = flip modifyMVar_ (pure . addEntry e) . unTempLog
 where
  addEntry e es
    | length es + 1 > maxTempEntries = es
    | otherwise                      = e : es

temporaryEntries' :: TempLog e -> IO [e]
temporaryEntries' (TempLog var) = do
  entries <- reverse <$> takeMVar var
  putMVar var []
  return entries

applyTimeout :: Timeout
applyTimeout = 5000000

-- | Stores entry in the log and then blocks until log entry is committed.
waitApplyEntry
  :: (MonadIO m, TempLogM e m, LabelOptic' "completed" A_Lens e Completed)
  => e -> m ()
waitApplyEntry entry = do
  addTemporaryEntry entry

  liftIO $ do
    timer <- Timer.newIO
    Timer.reset timer applyTimeout
    -- Wait until either something is put in the TMVar
    -- or the timeout is finished.
    atomically $ Timer.await timer <|> awaitCompleted
 where
  awaitCompleted = takeTMVar . fromJust . unCompleted $ entry ^. #completed
