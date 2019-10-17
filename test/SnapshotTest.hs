module SnapshotTest
  ( tests
  )
where

import Control.Concurrent.STM
import Control.Lens
import Control.Monad
import Data.Binary
import Data.List (nub, sort)
import HaskKV.Snapshot.All
import HaskKV.Types
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory
import System.FilePath

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BL

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
      createSnapshot' 100 1 manager
      doesFileExist (path </> partialFilename 100 1)
        >>= (@? "File doesn't exist")

testWriteAndSave :: TestTree
testWriteAndSave = withResource (openFolder "test2") closeFolder $ \getPath ->
  testCase "Writes to snapshot" $ do
    path    <- getPath
    manager <- newSnapshotManager $ Just path
    createSnapshot' 101 1 manager
    let
      snapData   = "Sample text"
      snapData'  = "This overrides snapData"
      snapData'' = "This doesn't override and gets ignored instead"
    writeSnapshot' 0  (C.pack snapData)   101 manager
    writeSnapshot' 0  (C.pack snapData')  101 manager
    writeSnapshot' 20 (C.pack snapData'') 101 manager
    saveSnapshot' 101 manager
    s <- readFile (path </> completedFilename 101 1)
    s @?= snapData'

testSaveRemovesOlderSnapshots :: TestTree
testSaveRemovesOlderSnapshots =
  withResource (openFolder "test3") closeFolder $ \getPath ->
    testCase "Save removes older snapshots" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      createSnapshot' 102 1 manager
      createSnapshot' 103 1 manager
      createSnapshot' 104 1 manager
      createSnapshot' 106 1 manager
      saveSnapshot' 104 manager
      createSnapshot' 105 1 manager
      saveSnapshot' 105 manager
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
      createSnapshot' 101 1 manager
      let
        text        = "Sample text" :: String
        encodedData = B.concat . BL.toChunks . encode $ text
      writeSnapshot' 0 encodedData 101 manager
      saveSnapshot' 101 manager
      snap  <- readSnapshot' 101 manager :: IO (Maybe String)
      snap' <- readSnapshot' 101 manager :: IO (Maybe String)
      snap @?= snap'

testSnapshotLoading :: TestTree
testSnapshotLoading =
  withResource (openFolder "snapshot_dir_test") closeFolder $ \getPath ->
    testCase "Tests snapshot loading from directory" $ do
      path    <- getPath
      manager <- newSnapshotManager $ Just path
      createSnapshot' 103 1 manager
      createSnapshot' 104 1 manager
      createSnapshot' 102 1 manager
      let snapData = "Sample text"
      writeSnapshot' 0 (C.pack snapData) 102 manager
      saveSnapshot' 102 manager
      createSnapshot' 105 1 manager

      atomically $ modifyTVar (_snapshots manager) $ \s ->
        s { _partial = sort (_partial s) }
      snapshots <- readTVarIO $ _snapshots manager
      closeSnapshotManager manager

      manager' <- newSnapshotManager $ Just path
      atomically $ modifyTVar (_snapshots manager') $ \s ->
        s { _partial = sort (_partial s) }
      snapshots' <- readTVarIO $ _snapshots manager'
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
      createSnapshot' 101 1 manager
      writeSnapshot' 0 (C.pack snapData) 101 manager
      saveSnapshot' 101 manager

      let sids = [1, 2, 3, 4] :: [SID]
      mapM_ (\sid -> createSnapshot' (sidToIdx sid) 1 manager) sids
      readLoop sids manager
      closeSnapshotManager manager
      files <- mapM (readSID path) sids
      length (nub files) @?= 1
      head files @?= snapData
 where
  sidToIdx = LogIndex . unSID
  readSID path = readFile . (path </>) . flip partialFilename 1 . sidToIdx
  readLoop []           _       = return ()
  readLoop (sid : sids) manager = do
    chunk <- readChunk' 9 sid manager
    forM_ chunk
      $ \c -> writeSnapshot'
          (c ^. #_offset)
          (_data c)
          (sidToIdx sid)
          manager
    case _type <$> chunk of
      Just FullChunk -> readLoop (sids ++ [sid]) manager
      Just EndChunk  -> readLoop sids manager
      _              -> error "Invalid chunk"
