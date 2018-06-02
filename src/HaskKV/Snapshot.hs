module HaskKV.Snapshot where

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
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
    { _completed :: Maybe Snapshot
    , _partial   :: [Snapshot]
    }

class HasSnapshotManager c where
    getSnapshotManager :: c -> TVar SnapshotManager

instance HasSnapshotManager (TVar SnapshotManager) where
    getSnapshotManager = id

newSnapshotManager :: IO (TVar SnapshotManager)
newSnapshotManager = newTVarIO SnapshotManager
    { _completed = Nothing
    , _partial   = []
    }

createSnapshotImpl :: (MonadIO m, MonadReader c m, HasSnapshotManager c)
                   => Int
                   -> m ()
createSnapshotImpl index =
    fmap getSnapshotManager ask >>= \manager -> liftIO $ do
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

writeSnapshotImpl :: (MonadIO m, MonadReader c m, HasSnapshotManager c)
                  => B.ByteString
                  -> Int
                  -> m ()
writeSnapshotImpl snapData index = mapM_ (put snapData)
                                 . fmap _file
                                 . find ((== index) . _index)
                                 . _partial
                               =<< liftIO . readTVarIO
                               =<< fmap getSnapshotManager ask
  where
    put snapData handle = liftIO $ do
        B.hPut handle snapData
        hFlush handle

saveSnapshotImpl :: (MonadIO m, MonadReader c m, HasSnapshotManager c)
                 => Int
                 -> m ()
saveSnapshotImpl index =
    fmap getSnapshotManager ask >>= \managerVar -> liftIO $ do
        manager <- readTVarIO managerVar
        let completed = _completed manager
            snap      = find ((== index) . _index) . _partial $ manager
        forM_ snap $ \snap@Snapshot{ _index = index } -> do
            completed' <- if maybe False ((< index) . _index) completed
                then const (Just snap) <$> mapM_ (hClose . _file) completed
                else return completed

            forM_ (_partial manager) $ \partial ->
                when (_index partial < index) $ hClose (_file partial)
            let partial' = filter ((< index) . _index)
                         . _partial
                         $ manager

            atomically $ writeTVar managerVar SnapshotManager
                { _completed = completed', _partial = partial' }
