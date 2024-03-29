module HaskKV.Utils where

import Control.Concurrent.MVar (MVar, putMVar, takeMVar)
import Control.Concurrent.STM
import Control.Monad.IO.Class (MonadIO (..))
import Data.Binary (Binary, decodeFileOrFail, encode)
import Data.Conduit (ConduitM, awaitForever, yield)
import HaskKV.Types (FileSize (FileSize), SID)
import System.IO (IOMode (WriteMode), hFileSize, hFlush, withFile)
import UnliftIO.Exception (SomeException, handle)

import qualified Data.ByteString.Lazy as BL
import qualified Data.Heap as H

stateMVar :: (s -> (a, s)) -> MVar s -> IO a
stateMVar f v = do
  s <- takeMVar v
  let (r, s') = f s
  putMVar v s'
  return r

stateTVarIO :: (s -> (a, s)) -> TVar s -> IO a
stateTVarIO f v = atomically $ do
  s <- readTVar v
  let (r, s') = f s
  writeTVar v s'
  return r

modifyTVarIO :: (a -> a) -> TVar a -> IO ()
modifyTVarIO f v = atomically $ modifyTVar' v f

minHeapMaybe :: H.Heap a -> Maybe a
minHeapMaybe h
  | H.null h  = Nothing
  | otherwise = Just $ H.minimum h

sourceTBQueue :: (MonadIO m) => TBQueue o -> ConduitM i o m b
sourceTBQueue q = go
 where
  go = do
    e <- liftIO $ atomically $ readTBQueue q
    yield e
    go
{-# INLINABLE sourceTBQueue #-}

sinkTBQueue :: (MonadIO m) => TBQueue a -> ConduitM a o m ()
sinkTBQueue q = awaitForever (liftIO . atomically . writeTBQueue q)
{-# INLINABLE sinkTBQueue #-}

sourceTBQueueOne :: (MonadIO m) => TBQueue o -> ConduitM i o m ()
sourceTBQueueOne q = do
  e <- liftIO $ atomically $ readTBQueue q
  yield e
  return ()

persistBinary :: (Binary b) => (SID -> FilePath) -> SID -> b -> IO FileSize
persistBinary filename sid binary =
  withFile (filename sid) WriteMode $ \file -> do
    BL.hPut file $ encode binary
    hFlush file
    FileSize . fromIntegral <$> hFileSize file
{-# INLINABLE persistBinary #-}

loadBinary :: (Binary b) => (SID -> FilePath) -> SID -> IO (Maybe b)
loadBinary filename = handle (\(_ :: SomeException) -> pure Nothing)
                    . fmap (either (const Nothing) Just)
                    . decodeFileOrFail
                    . filename
