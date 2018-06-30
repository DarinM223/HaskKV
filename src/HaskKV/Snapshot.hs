{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Snapshot where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary hiding (get)
import Data.List
import Data.Maybe
import GHC.IO.Handle
import GHC.Records
import HaskKV.Types
import System.Directory
import System.FilePath
import System.IO
import Text.Read (readMaybe)

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM

class (Binary s) => HasSnapshotType s (m :: * -> *) | m -> s

data SnapshotChunkType = FullChunk | EndChunk deriving (Show, Eq)

data SnapshotChunk = SnapshotChunk
    { _data   :: B.ByteString
    , _type   :: SnapshotChunkType
    , _offset :: Int
    , _index  :: LogIndex
    , _term   :: Int
    } deriving (Show, Eq)

class (Binary s) => SnapshotM s m | m -> s where
    createSnapshot :: LogIndex -> Int -> m ()
    writeSnapshot  :: Int -> B.ByteString -> LogIndex -> m ()
    saveSnapshot   :: LogIndex -> m ()
    readSnapshot   :: LogIndex -> m (Maybe s)
    hasChunk       :: SID -> m Bool
    readChunk      :: Int -> SID -> m (Maybe SnapshotChunk)
    snapshotInfo   :: m (Maybe (LogIndex, Int))

data Snapshot = Snapshot
    { _file     :: Handle
    , _index    :: LogIndex
    , _term     :: Int
    , _filepath :: FilePath
    , _offset   :: Int
    } deriving (Show, Eq)

instance Ord Snapshot where
    compare s1 s2 = compare (getField @"_index" s1) (getField @"_index" s2)

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
    files <- catch (getDirectoryContents path) $ \(_ :: SomeException) ->
        return []
    partial <- mapM (toPartial . (path </>)) . filter isPartial $ files
    completed <- mapM (toCompleted . (path </>)) . filter isCompleted $ files
    newTVarIO Snapshots
        { _completed = listToMaybe completed
        , _partial   = partial
        , _chunks    = IM.empty
        }
  where
    toSnapshot mode path = do
        let Just (index, term) = splitFilename . fileBase $ path
        handle <- openFile path mode
        fileSize <- hFileSize handle
        let offset = if fileSize > 0 then fromIntegral fileSize else 0
        return Snapshot
            { _file     = handle
            , _index    = index
            , _term     = term
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

    createSnapshot i t = liftIO . createSnapshotImpl i t
                     =<< asks getSnapshotManager
    writeSnapshot o d i = liftIO . writeSnapshotImpl o d i
                      =<< asks getSnapshotManager
    saveSnapshot i = liftIO . saveSnapshotImpl i =<< asks getSnapshotManager
    readSnapshot i = liftIO . readSnapshotImpl i =<< asks getSnapshotManager
    hasChunk i = liftIO . hasChunkImpl i =<< asks getSnapshotManager
    readChunk a i = liftIO . readChunkImpl a i =<< asks getSnapshotManager
    snapshotInfo = liftIO . snapshotInfoImpl =<< asks getSnapshotManager

createSnapshotImpl :: LogIndex -> Int -> SnapshotManager -> IO ()
createSnapshotImpl index term manager = do
    handle <- openFile filename WriteMode
    atomically $ modifyTVar (_snapshots manager) $ \s ->
        let snap = Snapshot
                { _file     = handle
                , _index    = index
                , _term     = term
                , _filepath = filename
                , _offset   = 0
                }
        in s { _partial = snap:_partial s }
  where
    filename = _directoryPath manager </> partialFilename index term

writeSnapshotImpl :: Int -> B.ByteString -> LogIndex -> SnapshotManager -> IO ()
writeSnapshotImpl offset snapData index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    partial' <- mapM (putAndUpdate offset snapData index) (_partial snapshots)
    atomically $ modifyTVar (_snapshots manager) $ \s ->
        s { _partial = partial' }
  where
    putAndUpdate offset snapData index snap
        | matchesIndexAndOffset index offset snap =
            putAndReturnOffset snapData snap
        | index == getField @"_index" snap && offset == 0 = do
            hSetFileSize (_file snap) 0
            hSetPosn $ HandlePosn (_file snap) 0
            putAndReturnOffset snapData snap
        | otherwise = return snap
      where
        matchesIndexAndOffset index offset snap
            = index == getField @"_index" snap
           && offset == getField @"_offset" snap

        putAndReturnOffset snapData snap = do
            B.hPut (_file snap) snapData
            hFlush $ _file snap
            offset <- getPos <$> hGetPosn (_file snap)
            return (snap { _offset = offset } :: Snapshot)

readSnapshotImpl :: (Binary s) => LogIndex -> SnapshotManager -> IO (Maybe s)
readSnapshotImpl index manager =
    handle (\(_ :: SomeException) -> return Nothing) $ do
        snapshots <- readTVarIO $ _snapshots manager
        case _completed snapshots of
            Just Snapshot{ _index = i, _filepath = path } | i == index -> do
                file <- openFile path ReadMode
                contents <- BL.hGetContents file
                return $ Just $ decode contents
            _ -> return Nothing

hasChunkImpl :: SID -> SnapshotManager -> IO Bool
hasChunkImpl (SID i) manager = do
    snapshots <- readTVarIO $ _snapshots manager
    case IM.lookup i $ _chunks snapshots of
        Just handle -> not <$> hIsEOF handle
        Nothing     -> return False

readChunkImpl :: Int -> SID -> SnapshotManager -> IO (Maybe SnapshotChunk)
readChunkImpl amount (SID sid) manager = do
    snapshots <- readTVarIO $ _snapshots manager
    case _completed snapshots of
        Just snap@Snapshot{ _filepath = filepath } -> do
            (chunk, chunks') <- flip runStateT (_chunks snapshots) $ do
                handle <- IM.lookup sid <$> get >>= \case
                    Just handle -> return handle
                    Nothing -> do
                        handle <- liftIO $ openFile filepath ReadMode
                        modify $ IM.insert sid handle
                        return handle
                offset <- liftIO . fmap getPos $ hGetPosn handle
                chunk <- liftIO
                       . fmap (B.concat . BL.toChunks)
                       $ BL.hGet handle amount
                chunkType <- liftIO (hIsEOF handle) >>= \case
                    True -> do
                        liftIO $ hClose handle
                        modify (IM.delete sid)
                        return EndChunk
                    False -> return FullChunk
                return SnapshotChunk
                    { _data   = chunk
                    , _type   = chunkType
                    , _offset = offset
                    , _index  = getField @"_index" snap
                    , _term   = getField @"_term" snap
                    }
            atomically $ modifyTVar (_snapshots manager) $ \snaps ->
                snaps { _chunks = chunks' }
            return $ Just chunk
        _ -> return Nothing

saveSnapshotImpl :: LogIndex -> SnapshotManager -> IO ()
saveSnapshotImpl index manager = do
    snapshots <- readTVarIO $ _snapshots manager
    let snap = find ((== index) . getField @"_index") . _partial $ snapshots

    forM_ snap $ \snap -> do
        -- Close and rename snapshot file as completed.
        hClose $ _file snap
        let index = getField @"_index" snap
            term  = getField @"_term" snap
            path' = replaceFileName (_filepath snap)
                                    (completedFilename index term)
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

snapshotInfoImpl :: SnapshotManager -> IO (Maybe (LogIndex, Int))
snapshotInfoImpl manager = do
    snapshots <- readTVarIO $ _snapshots manager
    let index = getField @"_index" <$> _completed snapshots
        term  = getField @"_term" <$> _completed snapshots
    return $ (,) <$> index <*> term

partialFilename :: LogIndex -> Int -> String
partialFilename i t = (show i) ++ "_" ++ (show t) ++ ".partial.snap"

completedFilename :: LogIndex -> Int -> String
completedFilename i t = (show i) ++ "_" ++ (show t) ++ ".completed.snap"

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

splitFilename :: String -> Maybe (LogIndex, Int)
splitFilename s = (,) <$> index <*> term
  where
    i = findIndex (== '_') s
    index = fmap LogIndex . readMaybe =<< flip take s <$> i
    term = readMaybe =<< flip drop s . (+ 1) <$> i
