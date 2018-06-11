{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Snapshot where

import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary hiding (get)
import Data.List
import Data.Maybe
import GHC.IO.Handle
import GHC.Records
import System.Directory
import System.FilePath
import System.IO

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM

class (Binary s) => HasSnapshotType s (m :: * -> *) | m -> s

data SnapshotChunkType = FullChunk | EndChunk

data SnapshotChunk = SnapshotChunk
    { _data   :: BL.ByteString
    , _type   :: SnapshotChunkType
    , _offset :: Int
    }

class (Binary s) => SnapshotM s m | m -> s where
    createSnapshot :: Int -> m ()
    writeSnapshot  :: Int -> B.ByteString -> Int -> m ()
    saveSnapshot   :: Int -> m ()
    readSnapshot   :: Int -> m (Maybe s)
    readChunk      :: Int -> Int -> m (Maybe SnapshotChunk)

data Snapshot = Snapshot
    { _file     :: Handle
    , _index    :: Int
    , _filepath :: FilePath
    , _offset   :: Int
    } deriving (Show, Eq)

instance Ord Snapshot where
    compare s1 s2 = compare (_index s1) (_index s2)

data Snapshots = Snapshots
    { _completed :: Maybe Snapshot
    , _partial   :: [Snapshot]
    , _chunks    :: IM.IntMap Handle
    } deriving (Show, Eq)

data SnapshotManager = SnapshotManager
    { _snapshots     :: TVar Snapshots
    , _directoryPath :: FilePath
    }

class HasSnapshotManager r where
    getSnapshotManager :: r -> SnapshotManager

newSnapshotManager :: Maybe FilePath -> IO SnapshotManager
newSnapshotManager path = do
    let directoryPath = fromMaybe "" path
    snapshots <- loadSnapshots directoryPath
    return SnapshotManager
        { _snapshots = snapshots, _directoryPath = directoryPath }

closeSnapshotManager :: SnapshotManager -> IO ()
closeSnapshotManager manager = do
    snapshots <- readTVarIO $ _snapshots manager
    mapM_ (hClose . _file) (_completed snapshots)
    mapM_ (hClose . _file) (_partial snapshots)

loadSnapshots :: FilePath -> IO (TVar Snapshots)
loadSnapshots path = do
    files <- getDirectoryContents path
    partial <- mapM (toPartial . (path </>)) . filter isPartial $ files
    completed <- mapM (toCompleted . (path </>)) . filter isCompleted $ files
    newTVarIO Snapshots
        { _completed = listToMaybe completed
        , _partial   = partial
        , _chunks    = IM.empty
        }
  where
    toSnapshot mode path = do
        let index = read . fileBase $ path :: Int
        handle <- openFile path mode
        fileSize <- hFileSize handle
        let offset = if fileSize > 0 then fromIntegral fileSize else 0
        return Snapshot
            { _file     = handle
            , _index    = index
            , _filepath = path
            , _offset   = offset
            }
    toPartial = toSnapshot AppendMode
    toCompleted = toSnapshot ReadMode

    isPartial = (== ".partial.snap") . fileExt
    isCompleted = (== ".completed.snap") . fileExt

newtype SnapshotT m a = SnapshotT { unSnapshotT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance
    ( MonadIO m
    , MonadReader r m
    , HasSnapshotManager r
    , HasSnapshotType s m
    ) => SnapshotM s (SnapshotT m) where

    createSnapshot i = liftIO . createSnapshotImpl i
                   =<< asks getSnapshotManager
    writeSnapshot o d i = liftIO . writeSnapshotImpl o d i
                      =<< asks getSnapshotManager
    saveSnapshot i = liftIO . saveSnapshotImpl i =<< asks getSnapshotManager
    readSnapshot i = liftIO . readSnapshotImpl i =<< asks getSnapshotManager
    readChunk a i = liftIO . readChunkImpl a i =<< asks getSnapshotManager

createSnapshotImpl :: Int -> SnapshotManager -> IO ()
createSnapshotImpl index manager = do
    handle <- openFile filename WriteMode
    atomically $ modifyTVar (_snapshots manager) $ \s ->
        let snap = Snapshot
                { _file     = handle
                , _index    = index
                , _filepath = filename
                , _offset   = 0
                }
        in s { _partial = snap:_partial s }
  where
    filename = _directoryPath manager </> partialFilename index

writeSnapshotImpl :: Int -> B.ByteString -> Int -> SnapshotManager -> IO ()
writeSnapshotImpl offset snapData index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    partial' <- mapM (putAndUpdate offset snapData index) (_partial snapshots)
    atomically $ modifyTVar (_snapshots manager) $ \s ->
        s { _partial = partial' }
  where
    putAndUpdate offset snapData index snap
        | index == _index snap && offset == getField @"_offset" snap =
            putAndReturnOffset snapData snap
        | index == _index snap && offset == 0 =
            hSetFileSize (_file snap) 0 >> putAndReturnOffset snapData snap
        | otherwise = return snap
      where
        putAndReturnOffset snapData snap = do
            B.hPut (_file snap) snapData
            hFlush $ _file snap
            offset <- getPos <$> hGetPosn (_file snap)
            return (snap { _offset = offset } :: Snapshot)

readSnapshotImpl :: (Binary s) => Int -> SnapshotManager -> IO (Maybe s)
readSnapshotImpl index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    case _completed snapshots of
        Just Snapshot{ _index = i, _file = file } | i == index ->
            pure . Just . decode =<< BL.hGetContents file
        _ -> return Nothing

readChunkImpl :: Int -> Int -> SnapshotManager -> IO (Maybe SnapshotChunk)
readChunkImpl amount sid manager = do
    snapshots <- readTVarIO $ _snapshots manager
    case _completed snapshots of
        Just Snapshot{ _filepath = filepath } -> do
            (chunk, chunks') <- flip runStateT (_chunks snapshots) $ do
                handle <- IM.lookup sid <$> get >>= \case
                    Just handle -> return handle
                    Nothing -> do
                        handle <- liftIO $ openFile filepath ReadMode
                        modify $ IM.insert sid handle
                        return handle
                offset <- liftIO . fmap getPos $ hGetPosn handle
                chunk <- liftIO $ BL.hGet handle amount
                chunkType <- liftIO (hIsEOF handle) >>= \case
                    True -> do
                        liftIO $ hClose handle
                        modify (IM.delete sid)
                        return EndChunk
                    False -> return FullChunk
                return SnapshotChunk
                    { _data = chunk, _type = chunkType, _offset = offset }
            atomically $ modifyTVar (_snapshots manager) $ \snaps ->
                snaps { _chunks = chunks' }
            return $ Just chunk
        _ -> return Nothing

saveSnapshotImpl :: Int -> SnapshotManager -> IO ()
saveSnapshotImpl index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    let snap = find ((== index) . _index) . _partial $ snapshots

    forM_ snap $ \snap@Snapshot{ _index = index } -> do
        -- Close and rename snapshot file as completed.
        hClose $ _file snap
        let path' = replaceFileName (_filepath snap) (completedFilename index)
        renameFile (_filepath snap) path'
        reopened <- openFile path' ReadMode
        let snap' = snap { _file = reopened, _filepath = path' }

        -- Replace the existing completed snapshot file if its
        -- index is smaller than the saving snapshot's index.
        completed' <- case _completed snapshots of
            Just old@Snapshot{ _index = i } | i < index ->
                removeSnapshot old >> pure (Just snap')
            Nothing   -> pure (Just snap')
            completed -> removeSnapshot snap' >> pure completed

        -- Remove partial snapshots with an index smaller than
        -- the saving snapshot's index.
        partial' <- filterM (filterSnapshot index)
                  . _partial
                  $ snapshots

        atomically $ modifyTVar (_snapshots manager) $ \s ->
            s { _completed = completed', _partial = partial' }
  where
    removeSnapshot snap = do
        hClose $ _file snap
        removeFile $ _filepath snap

    filterSnapshot index = \case
        snap@Snapshot{ _index = i } | i < index ->
            removeSnapshot snap >> pure False
        Snapshot{ _index = i } | i > index -> pure True
        _                                  -> pure False

partialFilename :: Int -> String
partialFilename i = show i ++ ".partial.snap"

completedFilename :: Int -> String
completedFilename i = show i ++ ".completed.snap"

fileBase :: FilePath -> String
fileBase path
    | hasExtension path = fileBase $ takeBaseName path
    | otherwise         = path

fileExt :: FilePath -> String
fileExt = go ""
  where
    go fullExt path = case splitExtension path of
        (_, "")           -> fullExt
        (incomplete, ext) -> go (ext ++ fullExt) incomplete

getPos :: HandlePosn -> Int
getPos (HandlePosn _ pos) = fromIntegral pos
