module HaskKV.Utils where

import Control.Concurrent.STM
import qualified Data.Heap as H

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
