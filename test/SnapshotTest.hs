module SnapshotTest (tests) where

import Control.Concurrent.STM
import Control.Monad
import Data.List
import GHC.Records
import HaskKV.Snapshot
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory
import System.FilePath

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as C

openFolder :: FilePath -> IO FilePath
openFolder path = createDirectory path >> return path

closeFolder :: FilePath -> IO ()
closeFolder = removeDirectoryRecursive

tests :: TestTree
tests = testGroup "Snapshot tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCreateSnapshot
    , testWriteAndSave
    , testSaveRemovesOlderSnapshots
    , testSnapshotLoading
    , testReadChunks
    ]

testCreateSnapshot :: TestTree
testCreateSnapshot =
    withResource (openFolder "test1") closeFolder $ \getPath ->
        testCase "Creates snapshot" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 100 manager
            doesFileExist (path </> partialFilename 100) >>=
                (@? "File doesn't exist")

testWriteAndSave :: TestTree
testWriteAndSave =
    withResource (openFolder "test2") closeFolder $ \getPath ->
        testCase "Writes to snapshot" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 101 manager
            let snapData   = "Sample text"
                snapData'  = "This overrides snapData"
                snapData'' = "This doesn't override and gets ignored instead"
            writeSnapshotImpl 0 (C.pack snapData) 101 manager
            writeSnapshotImpl 0 (C.pack snapData') 101 manager
            writeSnapshotImpl 20 (C.pack snapData'') 101 manager
            saveSnapshotImpl 101 manager
            s <- readFile (path </> completedFilename 101)
            s @?= snapData'

testSaveRemovesOlderSnapshots :: TestTree
testSaveRemovesOlderSnapshots =
    withResource (openFolder "test3") closeFolder $ \getPath ->
        testCase "Save removes older snapshots" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 102 manager
            createSnapshotImpl 103 manager
            createSnapshotImpl 104 manager
            createSnapshotImpl 106 manager
            saveSnapshotImpl 104 manager
            createSnapshotImpl 105 manager
            saveSnapshotImpl 105 manager
            doesFileExist (path </> partialFilename 102) >>=
                (@? "File exists") . not
            doesFileExist (path </> partialFilename 103) >>=
                (@? "File exists") . not
            doesFileExist (path </> completedFilename 104) >>=
                (@? "File exists") . not
            doesFileExist (path </> completedFilename 105) >>=
                (@? "File doesn't exist")
            doesFileExist (path </> partialFilename 106) >>=
                (@? "File doesn't exist")

testSnapshotLoading :: TestTree
testSnapshotLoading =
    withResource (openFolder "snapshot_dir_test") closeFolder $ \getPath ->
        testCase "Tests snapshot loading from directory" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 103 manager
            createSnapshotImpl 104 manager
            createSnapshotImpl 102 manager
            let snapData = "Sample text"
            writeSnapshotImpl 0 (C.pack snapData) 102 manager
            saveSnapshotImpl 102 manager
            createSnapshotImpl 105 manager

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
            s <- readFile (path </> completedFilename 102)
            s @?= snapData

testReadChunks :: TestTree
testReadChunks =
    withResource (openFolder "snapshot_readchunk") closeFolder $ \getPath ->
        testCase "Tests reads chunks from snapshot" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            let snapData = replicate 1000 'a'
            createSnapshotImpl 101 manager
            writeSnapshotImpl 0 (C.pack snapData) 101 manager
            saveSnapshotImpl 101 manager
            let sids = [1, 2, 3, 4]
            mapM_ (`createSnapshotImpl` manager) sids
            readLoop sids manager
            closeSnapshotManager manager
            files <- mapM (readFile . (path </>) . partialFilename) sids
            length (nub files) @?= 1
            head files @?= snapData
  where
    toStrict = B.concat . BL.toChunks
    toByteString = toStrict . _data
    readLoop [] _ = return ()
    readLoop (sid:sids) manager = do
        chunk <- readChunkImpl 9 sid manager
        forM_ chunk $ \c ->
            writeSnapshotImpl (getField @"_offset" c)
                              (toByteString c)
                              sid
                              manager
        case _type <$> chunk of
            Just FullChunk -> readLoop (sids ++ [sid]) manager
            Just EndChunk  -> readLoop sids manager
            _              -> error "Invalid chunk"
