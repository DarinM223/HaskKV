module HaskKV.Snapshot.Instances where

import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary (Binary, decode)
import Data.Foldable (find)
import GHC.IO.Handle
import GHC.Records
import HaskKV.Snapshot.Types
import HaskKV.Snapshot.Utils
import HaskKV.Types
import System.Directory
import System.FilePath
import System.IO

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM

mkSnapshotM :: (Binary s, MonadIO m) => SnapshotManager -> SnapshotM s m
mkSnapshotM manager = SnapshotM
  { createSnapshot = createSnapshot' manager
  , writeSnapshot  = writeSnapshot' manager
  , saveSnapshot   = saveSnapshot' manager
  , readSnapshot   = readSnapshot' manager
  , hasChunk       = hasChunk' manager
  , readChunk      = readChunk' manager
  , snapshotInfo   = snapshotInfo' manager
  }

createSnapshot' :: MonadIO m => SnapshotManager -> LogIndex -> LogTerm -> m ()
createSnapshot' manager index term = liftIO $ do
  let filename = _directoryPath manager </> partialFilename index term
  handle <- openFile filename WriteMode
  atomically $ modifyTVar (_snapshots manager) $ \s ->
    let
      snap = Snapshot
        { _file     = handle
        , _index    = index
        , _term     = term
        , _filepath = filename
        , _offset   = 0
        }
    in s { _partial = snap : _partial s }

writeSnapshot' :: MonadIO m
               => SnapshotManager -> FilePos -> B.ByteString -> LogIndex -> m ()
writeSnapshot' manager offset snapData index = liftIO $ do
  snapshots <- readTVarIO $ _snapshots manager
  partial'  <- mapM (putAndUpdate offset snapData index) (_partial snapshots)
  atomically $ modifyTVar (_snapshots manager) $ \s -> s { _partial = partial' }
 where
  putAndUpdate offset snapData index snap
    | matchesIndexAndOffset index offset snap = putAndReturnOffset snapData snap
    | index == getField @"_index" snap && offset == 0 = do
      hSetFileSize (_file snap) 0
      hSetPosn $ HandlePosn (_file snap) 0
      putAndReturnOffset snapData snap
    | otherwise = return snap
   where
    matchesIndexAndOffset index offset snap =
      index == getField @"_index" snap && offset == getField @"_offset" snap

    putAndReturnOffset snapData snap = do
      B.hPut (_file snap) snapData
      hFlush $ _file snap
      offset <- getPos <$> hGetPosn (_file snap)
      return (snap { _offset = offset } :: Snapshot)

readSnapshot' :: (Binary s, MonadIO m)
              => SnapshotManager -> LogIndex -> m (Maybe s)
readSnapshot' manager index =
  liftIO $ handle (\(_ :: SomeException) -> return Nothing) $ do
    snapshots <- readTVarIO $ _snapshots manager
    case _completed snapshots of
      Just Snapshot { _index = i, _filepath = path } | i == index ->
        Just . decode <$> BL.readFile path
      _ -> return Nothing

hasChunk' :: MonadIO m => SnapshotManager -> SID -> m Bool
hasChunk' manager (SID i) = liftIO $ do
  snapshots <- readTVarIO $ _snapshots manager
  case IM.lookup i $ _chunks snapshots of
    Just handle -> not <$> hIsEOF handle
    Nothing     -> return False

readChunk' :: MonadIO m
           => SnapshotManager -> Int -> SID -> m (Maybe SnapshotChunk)
readChunk' manager amount (SID sid) = liftIO $ do
  snapshots <- readTVarIO $ _snapshots manager
  case _completed snapshots of
    Just snap@Snapshot { _filepath = filepath } -> do
      (chunk, chunks') <- flip runStateT (_chunks snapshots) $ do
        handle <- IM.lookup sid <$> get >>= \case
          Just handle -> return handle
          Nothing     -> do
            handle <- liftIO $ openFile filepath ReadMode
            modify $ IM.insert sid handle
            return handle
        offset <- liftIO . fmap getPos $ hGetPosn handle
        chunk <- liftIO . fmap (B.concat . BL.toChunks) $ BL.hGet handle amount
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

saveSnapshot' :: MonadIO m => SnapshotManager -> LogIndex -> m ()
saveSnapshot' manager index = liftIO $ do
  snapshots <- readTVarIO $ _snapshots manager
  let snap = find ((== index) . getField @"_index") . _partial $ snapshots

  forM_ snap $ \snap -> do
    -- Close and rename snapshot file as completed.
    hClose $ _file snap
    let
      index = getField @"_index" snap
      term  = getField @"_term" snap
      path' = replaceFileName (_filepath snap) (completedFilename index term)
    renameFile (_filepath snap) path'
    reopened <- openFile path' ReadMode
    let snap' = snap { _file = reopened, _filepath = path' }

    -- Replace the existing completed snapshot file if its
    -- index is smaller than the saving snapshot's index.
    completed' <- case _completed snapshots of
      Just old@Snapshot { _index = i } | i < index ->
        removeSnapshot old >> pure (Just snap')
      Nothing   -> pure (Just snap')
      completed -> removeSnapshot snap' >> pure completed

    -- Remove partial snapshots with an index smaller than
    -- the saving snapshot's index.
    partial' <- filterM (filterSnapshot index) . _partial $ snapshots

    atomically $ modifyTVar (_snapshots manager) $ \s ->
      s { _completed = completed', _partial = partial' }
 where
  removeSnapshot snap = do
    hClose $ _file snap
    removeFile $ _filepath snap

  filterSnapshot index = \case
    snap@Snapshot { _index = i } | i < index ->
      removeSnapshot snap >> pure False
    Snapshot { _index = i } | i > index -> pure True
    _ -> pure False

snapshotInfo' :: MonadIO m
              => SnapshotManager -> m (Maybe (LogIndex, LogTerm, FileSize))
snapshotInfo' manager = liftIO $ do
  snapshots <- readTVarIO $ _snapshots manager
  size <- maybe (pure Nothing) (fmap (Just . fromIntegral) . hFileSize . _file)
    $ _completed snapshots
  let
    index = getField @"_index" <$> _completed snapshots
    term  = getField @"_term" <$> _completed snapshots
  return $ (,,) <$> index <*> term <*> size
