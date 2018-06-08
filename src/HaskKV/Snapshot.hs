{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Snapshot where

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Data.List
import Data.Maybe
import System.Directory
import System.FilePath
import System.IO

import qualified Data.ByteString as B

class SnapshotM m where
    createSnapshot :: Int -> m ()
    writeSnapshot  :: B.ByteString -> Int -> m ()
    saveSnapshot   :: Int -> m ()

data Snapshot = Snapshot
    { _file     :: Handle
    , _index    :: Int
    , _filepath :: FilePath
    } deriving (Show)

data Snapshots = Snapshots
    { _completed :: Maybe Snapshot
    , _partial   :: [Snapshot]
    }

data SnapshotManager = SnapshotManager
    { _snapshots     :: TVar Snapshots
    , _directoryPath :: FilePath
    }

class HasSnapshotManager r where
    getSnapshotManager :: r -> SnapshotManager

newSnapshotManager :: Maybe FilePath -> IO SnapshotManager
newSnapshotManager path = do
    snapshots <- newTVarIO Snapshots
        { _completed = Nothing
        , _partial   = []
        }
    return SnapshotManager
        { _snapshots = snapshots, _directoryPath = fromMaybe "" path }

loadSnapshots :: FilePath -> TVar Snapshots -> IO ()
loadSnapshots path snapshots = undefined -- TODO(DarinM223): implement this

newtype SnapshotT m a = SnapshotT { unSnapshotT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance
    ( MonadIO m
    , MonadReader r m
    , HasSnapshotManager r
    ) => SnapshotM (SnapshotT m) where

    createSnapshot i = liftIO . createSnapshotImpl i
                   =<< asks getSnapshotManager
    writeSnapshot d i = liftIO . writeSnapshotImpl d i
                    =<< asks getSnapshotManager
    saveSnapshot i = liftIO . saveSnapshotImpl i =<< asks getSnapshotManager

createSnapshotImpl :: Int -> SnapshotManager -> IO ()
createSnapshotImpl index manager = do
    handle <- openFile filename WriteMode
    atomically $ modifyTVar (_snapshots manager) $ \s ->
        let snap = Snapshot
                { _file     = handle
                , _index    = index
                , _filepath = filename
                }
        in s { _partial = snap:_partial s }
  where
    filename = _directoryPath manager </> partialFilename index

writeSnapshotImpl :: B.ByteString -> Int -> SnapshotManager -> IO ()
writeSnapshotImpl snapData index = mapM_ (put snapData . _file)
                                 . find ((== index) . _index)
                                 . _partial
                               <=< readTVarIO
                                 . _snapshots
  where
    put snapData handle = do
        B.hPut handle snapData
        hFlush handle

saveSnapshotImpl :: Int -> SnapshotManager -> IO ()
saveSnapshotImpl index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    let snap = find ((== index) . _index) . _partial $ snapshots

    forM_ snap $ \snap@Snapshot{ _index = index } -> do
        -- Close and rename snapshot file as completed.
        hClose $ _file snap
        let path' = replaceFileName (_filepath snap) (completedFilename index)
            snap' = snap { _filepath = path' }
        renameFile (_filepath snap) (_filepath snap')

        -- Replace the existing completed snapshot file if its
        -- index is smaller than the saving snapshot's index.
        completed' <- case _completed snapshots of
            Just old@Snapshot{ _index = i } | i < index -> do
                removeSnapshot old
                return $ Just snap'
            Nothing   -> return $ Just snap'
            completed -> return completed

        -- Remove partial snapshots with an index smaller than
        -- the saving snapshot's index.
        partial' <- filterM (filterSnapshot index)
                  . _partial
                  $ snapshots

        atomically $ writeTVar (_snapshots manager) Snapshots
            { _completed = completed', _partial = partial' }
  where
    removeSnapshot snap = do
        hClose $ _file snap
        removeFile $ _filepath snap

    filterSnapshot index = \case
        snap@Snapshot{ _index = i } | i < index -> do
            removeSnapshot snap
            return False
        Snapshot{ _index = i } | i > index -> return True
        _                                  -> return False

partialFilename :: Int -> String
partialFilename i = show i ++ ".partial.snap"

completedFilename :: Int -> String
completedFilename i = show i ++ ".completed.snap"
