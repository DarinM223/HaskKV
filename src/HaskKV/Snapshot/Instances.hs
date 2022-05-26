{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Snapshot.Instances where

import Control.Concurrent.STM (atomically, readTVarIO, modifyTVar)
import Control.Exception (SomeException, handle)
import Control.Monad.Reader (MonadReader)
import Control.Monad.State
import Data.Binary (Binary, decode)
import Data.Foldable (find, for_)
import GHC.IO.Handle
import HaskKV.Snapshot.Types
import HaskKV.Snapshot.Utils (getPos, completedFilename, partialFilename)
import HaskKV.Types (FilePos, FileSize, LogIndex, LogTerm, SID (SID))
import Optics ((&), (%), (%~), (.~), (^.), At (at), ViewableOptic (gview))
import System.Directory (removeFile, renameFile)
import System.FilePath ((</>), replaceFileName)
import System.IO (IOMode (ReadMode, WriteMode), openFile)

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM

instance
  ( MonadIO m
  , MonadReader r m
  , HasSnapshotManager r
  , HasSnapshotType s m
  ) => SnapshotM s (SnapshotT m) where

  createSnapshot i t = gview snapshotManagerL >>= liftIO . createSnapshot' i t
  writeSnapshot o d i = gview snapshotManagerL >>= liftIO . writeSnapshot' o d i
  saveSnapshot i = gview snapshotManagerL >>= liftIO . saveSnapshot' i
  readSnapshot i = gview snapshotManagerL >>= liftIO . readSnapshot' i
  hasChunk i = gview snapshotManagerL >>= liftIO . hasChunk' i
  readChunk a i = gview snapshotManagerL >>= liftIO . readChunk' a i
  snapshotInfo = gview snapshotManagerL >>= liftIO . snapshotInfo'

createSnapshot' :: LogIndex -> LogTerm -> SnapshotManager -> IO ()
createSnapshot' index term manager = do
  handle <- openFile filename WriteMode
  atomically $ modifyTVar (manager ^. #snapshots) $ \s ->
    let snap = Snapshot
          { file     = handle
          , index    = index
          , term     = term
          , filepath = filename
          , offset   = 0
          }
    in s & #partial %~ (snap :)
  where filename = manager ^. #directoryPath </> partialFilename index term

writeSnapshot'
  :: FilePos -> B.ByteString -> LogIndex -> SnapshotManager -> IO ()
writeSnapshot' offset snapData index manager = do
  snapshots <- readTVarIO $ manager ^. #snapshots
  partial' <- traverse
    (putAndUpdate offset snapData index)
    (snapshots ^. #partial)
  atomically $ modifyTVar (manager ^. #snapshots) $ #partial .~ partial'
 where
  putAndUpdate offset snapData index snap
    | matchesIndexAndOffset index offset snap = putAndReturnOffset snapData snap
    | index == snap ^. #index && offset == 0 = do
      hSetFileSize (snap ^. #file) 0
      hSetPosn $ HandlePosn (snap ^. #file) 0
      putAndReturnOffset snapData snap
    | otherwise = return snap
   where
    matchesIndexAndOffset index offset snap =
      index == snap ^. #index && offset == snap ^. #offset

    putAndReturnOffset snapData snap = do
      B.hPut (snap ^. #file) snapData
      hFlush $ snap ^. #file
      offset <- getPos <$> hGetPosn (snap ^. #file)
      return $ snap & #offset .~ offset

readSnapshot' :: (Binary s) => LogIndex -> SnapshotManager -> IO (Maybe s)
readSnapshot' index manager =
  handle (\(_ :: SomeException) -> return Nothing) $ do
    snapshots <- readTVarIO $ manager ^. #snapshots
    case snapshots ^. #completed of
      Just s | s ^. #index == index ->
        Just . decode <$> BL.readFile (s ^. #filepath)
      _ -> return Nothing

hasChunk' :: SID -> SnapshotManager -> IO Bool
hasChunk' (SID i) manager = do
  snapshots <- readTVarIO $ manager ^. #snapshots
  case snapshots ^. #chunks % at i of
    Just handle -> not <$> hIsEOF handle
    Nothing     -> return False

readChunk' :: Int -> SID -> SnapshotManager -> IO (Maybe SnapshotChunk)
readChunk' amount (SID sid) manager = do
  snapshots <- readTVarIO $ manager ^. #snapshots
  case snapshots ^. #completed of
    Just snap -> do
      (chunk, chunks') <- flip runStateT (snapshots ^. #chunks) $ do
        handle <- IM.lookup sid <$> get >>= \case
          Just handle -> return handle
          Nothing     -> do
            handle <- liftIO $ openFile (snap ^. #filepath) ReadMode
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
          { chunkData = chunk
          , chunkType = chunkType
          , offset    = offset
          , index     = snap ^. #index
          , term      = snap ^. #term
          }
      atomically $ modifyTVar (manager ^. #snapshots) $ #chunks .~ chunks'
      return $ Just chunk
    _ -> return Nothing

saveSnapshot' :: LogIndex -> SnapshotManager -> IO ()
saveSnapshot' index manager = do
  snapshots <- readTVarIO $ manager ^. #snapshots
  let snap = find ((== index) . (^. #index)) $ snapshots ^. #partial

  for_ snap $ \snap -> do
    -- Close and rename snapshot file as completed.
    hClose $ snap ^. #file
    let path' = replaceFileName
                (snap ^. #filepath)
                (completedFilename index (snap ^. #term))
    renameFile (snap ^. #filepath) path'
    reopened <- openFile path' ReadMode
    let snap' = snap & #file .~ reopened & #filepath .~ path'

    -- Replace the existing completed snapshot file if its
    -- index is smaller than the saving snapshot's index.
    completed' <- case snapshots ^. #completed of
      Just old | old ^. #index < index ->
        removeSnapshot old >> pure (Just snap')
      Nothing   -> pure (Just snap')
      completed -> removeSnapshot snap' >> pure completed

    -- Remove partial snapshots with an index smaller than
    -- the saving snapshot's index.
    partial' <- filterM (filterSnapshot index) $ snapshots ^. #partial

    atomically $ modifyTVar (manager ^. #snapshots) $
      (#completed .~ completed') . (#partial .~ partial')
 where
  removeSnapshot snap = do
    hClose $ snap ^. #file
    removeFile $ snap ^. #filepath

  filterSnapshot index = \case
    snap | snap ^. #index < index ->
      removeSnapshot snap >> pure False
    snap | snap ^. #index > index -> pure True
    _ -> pure False

snapshotInfo' :: SnapshotManager -> IO (Maybe (LogIndex, LogTerm, FileSize))
snapshotInfo' manager = do
  snapshots <- readTVarIO $ manager ^. #snapshots
  size <- maybe
          (pure Nothing)
          (fmap (Just . fromIntegral) . hFileSize . (^. #file))
        $ snapshots ^. #completed
  let index = (^. #index) <$> snapshots ^. #completed
      term  = (^. #term) <$> snapshots ^. #completed
  return $ (,,) <$> index <*> term <*> size
