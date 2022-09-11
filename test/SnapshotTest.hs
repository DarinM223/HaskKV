{-# LANGUAGE OverloadedLabels #-}
module SnapshotTest (tests) where

import Control.Concurrent.STM
import Data.Binary
import Data.Foldable (for_, traverse_)
import Data.List (nub, sort)
import HaskKV.Snapshot.All
import HaskKV.Types
import Optics
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory
import System.FilePath

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BL
import qualified HaskKV.Snapshot.Instances as S

openFolder :: FilePath -> IO FilePath
openFolder path = createDirectory path >> return path

closeFolder :: FilePath -> IO ()
closeFolder = removeDirectoryRecursive

tests :: TestTree
tests = testGroup "Snapshot tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup
  "Unit tests"
  [ testCreateSnapshot
  , testWriteAndSave
  , testSaveRemovesOlderSnapshots
  , testReadSnapshotMultipleTimes
  , testSnapshotLoading
  , testReadChunks
  ]

testCreateSnapshot :: TestTree
testCreateSnapshot =
  withResource (openFolder "test1") closeFolder $ \getPath ->
    testCase "Creates snapshot" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      S.createSnapshot 100 1 manager
      doesFileExist (path </> partialFilename 100 1)
        >>= (@? "File doesn't exist")

testWriteAndSave :: TestTree
testWriteAndSave = withResource (openFolder "test2") closeFolder $ \getPath ->
  testCase "Writes to snapshot" $ do
    path    <- getPath
    manager <- newSnapshotManager $ Just path
    S.createSnapshot 101 1 manager
    let
      snapData   = "Sample text"
      snapData'  = "This overrides snapData"
      snapData'' = "This doesn't override and gets ignored instead"
    S.writeSnapshot 0  (C.pack snapData)   101 manager
    S.writeSnapshot 0  (C.pack snapData')  101 manager
    S.writeSnapshot 20 (C.pack snapData'') 101 manager
    S.saveSnapshot 101 manager
    s <- readFile (path </> completedFilename 101 1)
    s @?= snapData'

testSaveRemovesOlderSnapshots :: TestTree
testSaveRemovesOlderSnapshots =
  withResource (openFolder "test3") closeFolder $ \getPath ->
    testCase "Save removes older snapshots" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      S.createSnapshot 102 1 manager
      S.createSnapshot 103 1 manager
      S.createSnapshot 104 1 manager
      S.createSnapshot 106 1 manager
      S.saveSnapshot 104 manager
      S.createSnapshot 105 1 manager
      S.saveSnapshot 105 manager
      doesFileExist (path </> partialFilename 102 1)
        >>= (@? "File exists")
        .   not
      doesFileExist (path </> partialFilename 103 1)
        >>= (@? "File exists")
        .   not
      doesFileExist (path </> completedFilename 104 1)
        >>= (@? "File exists")
        .   not
      doesFileExist (path </> completedFilename 105 1)
        >>= (@? "File doesn't exist")
      doesFileExist (path </> partialFilename 106 1)
        >>= (@? "File doesn't exist")

testReadSnapshotMultipleTimes :: TestTree
testReadSnapshotMultipleTimes =
  withResource (openFolder "test4") closeFolder $ \getPath ->
    testCase "Can read snapshots multiple times" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      S.createSnapshot 101 1 manager
      let
        text        = "Sample text" :: String
        encodedData = B.concat . BL.toChunks . encode $ text
      S.writeSnapshot 0 encodedData 101 manager
      S.saveSnapshot 101 manager
      snap  <- S.readSnapshot 101 manager :: IO (Maybe String)
      snap' <- S.readSnapshot 101 manager :: IO (Maybe String)
      snap @?= snap'

testSnapshotLoading :: TestTree
testSnapshotLoading =
  withResource (openFolder "snapshot_dir_test") closeFolder $ \getPath ->
    testCase "Tests snapshot loading from directory" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      S.createSnapshot 103 1 manager
      S.createSnapshot 104 1 manager
      S.createSnapshot 102 1 manager
      let snapData = "Sample text"
      S.writeSnapshot 0 (C.pack snapData) 102 manager
      S.saveSnapshot 102 manager
      S.createSnapshot 105 1 manager

      atomically $ modifyTVar (manager ^. #snapshots) $ #partial %~ sort
      snapshots <- readTVarIO $ manager ^. #snapshots
      closeSnapshotManager manager

      manager' <- newSnapshotManager $ Just path
      atomically $ modifyTVar (manager' ^. #snapshots) $ #partial %~ sort
      snapshots' <- readTVarIO $ manager' ^. #snapshots
      show snapshots @?= show snapshots'

      -- Test that the completed file is not locked and can be read.
      s <- readFile (path </> completedFilename 102 1)
      s @?= snapData

testReadChunks :: TestTree
testReadChunks =
  withResource (openFolder "snapshot_readchunk") closeFolder $ \getPath ->
    testCase "Tests reads chunks from snapshot" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      let snapData = replicate 1000 'a'
      S.createSnapshot 101 1 manager
      S.writeSnapshot 0 (C.pack snapData) 101 manager
      S.saveSnapshot 101 manager

      let sids = [1, 2, 3, 4] :: [SID]
      traverse_ (\sid -> S.createSnapshot (sidToIdx sid) 1 manager) sids
      readLoop sids manager
      closeSnapshotManager manager
      files <- traverse (readSID path) sids
      length (nub files) @?= 1
      head files @?= snapData
 where
  sidToIdx = LogIndex . unSID
  readSID path = readFile . (path </>) . flip partialFilename 1 . sidToIdx
  readLoop []           _       = return ()
  readLoop (sid : sids) manager = do
    chunk <- S.readChunk 9 sid manager
    for_ chunk $ \c ->
      S.writeSnapshot (c ^. #offset) (c ^. #chunkData) (sidToIdx sid) manager
    case (^. #chunkType) <$> chunk of
      Just FullChunk -> readLoop (sids ++ [sid]) manager
      Just EndChunk  -> readLoop sids manager
      _              -> error "Invalid chunk"
