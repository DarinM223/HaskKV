module HaskKV.Snapshot where

import Control.Concurrent.STM
import Control.Monad.IO.Class
import Control.Monad
import Data.List
import System.IO

import qualified Data.ByteString as B

class SnapshotM m where
    createSnapshot :: Int -> m ()
    writeSnapshot  :: B.ByteString -> Int -> m ()
    saveSnapshot   :: Int -> m ()

data Snapshot = Snapshot
    { _file     :: Handle
    , _index    :: Int
    , _filepath :: String
    }

data SnapshotManager = SnapshotManager
    { _completed :: Snapshot
    , _partial   :: [Snapshot]
    }

createSnapshotImpl :: (MonadIO m) => Int -> TVar SnapshotManager -> m ()
createSnapshotImpl index manager = liftIO $ do
    handle <- openFile filename WriteMode
    atomically $ modifyTVar manager $ \m -> 
        let snap = Snapshot
                { _file     = handle
                , _index    = index
                , _filepath = filename
                }
        in m { _partial = snap:_partial m }
  where
    filename = show index ++ ".snap"

writeSnapshotImpl :: (MonadIO m)
                  => B.ByteString
                  -> Int
                  -> TVar SnapshotManager
                  -> m ()
writeSnapshotImpl snapData index = mapM_ (put snapData)
                                 . fmap _file
                                 . find ((== index) . _index)
                                 . _partial
                               <=< liftIO . readTVarIO
  where
    put snapData handle = liftIO $ do
        B.hPut handle snapData
        hFlush handle

saveSnapshotImpl :: (MonadIO m) => Int -> TVar SnapshotManager -> m ()
saveSnapshotImpl index managerVar = liftIO $ do
    manager <- readTVarIO managerVar
    let completed = _completed manager
        snap      = find ((== index) . _index) . _partial $ manager
    forM_ snap $ \snap@Snapshot{ _index = index } -> do
        completed' <- if _index completed < index
            then const snap <$> hClose (_file completed)
            else return completed

        forM_ (_partial manager) $ \partial ->
            when (_index partial < index) $ hClose (_file partial)
        let partial' = filter ((< index) . _index)
                     . _partial
                     $ manager

        atomically $ writeTVar managerVar SnapshotManager
            { _completed = completed', _partial = partial' }
