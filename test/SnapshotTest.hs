module SnapshotTest (tests) where

import Control.Concurrent.STM
import Data.List
import HaskKV.Snapshot
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory
import System.FilePath

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
            return ()

testWriteAndSave :: TestTree
testWriteAndSave =
    withResource (openFolder "test2") closeFolder $ \getPath ->
        testCase "Writes to snapshot" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 101 manager
            let snapData = "Sample text"
            writeSnapshotImpl (C.pack snapData) 101 manager
            saveSnapshotImpl 101 manager
            closeSnapshotManager manager
            s <- readFile (path </> completedFilename 101)
            s @?= snapData

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
            return ()

testSnapshotLoading :: TestTree
testSnapshotLoading =
    withResource (openFolder "snapshot_dir_test") closeFolder $ \getPath ->
        testCase "Tests snapshot loading from directory" $ do
            path <- getPath
            manager <- newSnapshotManager $ Just path
            createSnapshotImpl 103 manager
            createSnapshotImpl 104 manager
            createSnapshotImpl 102 manager
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
