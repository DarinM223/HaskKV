module HaskKV.Utils where

import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Conduit
import qualified Data.Heap as H
import qualified Data.STM.RollingQueue as RQ

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

sourceRollingQueue :: (MonadIO m) => RQ.RollingQueue o -> ConduitM i o m b
sourceRollingQueue q = go
  where
    go = do
        (e, _) <- liftIO $ atomically $ RQ.read q
        yield e
        go

sinkRollingQueue :: (MonadIO m) => RQ.RollingQueue a -> ConduitM a o m ()
sinkRollingQueue q =
    awaitForever (liftIO . atomically . RQ.write q)

sourceRQOne :: (MonadIO m) => RQ.RollingQueue o -> ConduitM i o m ()
sourceRQOne q = do
    (e, _) <- liftIO $ atomically $ RQ.read q
    yield e
    return ()
