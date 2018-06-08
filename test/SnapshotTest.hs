module SnapshotTest (tests) where

import HaskKV.Snapshot
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory

import qualified Data.ByteString.Char8 as C

openFolder :: FilePath -> IO FilePath
openFolder path = createDirectory path >> return path

closeFolder :: FilePath -> IO ()
closeFolder = removeDirectoryRecursive

tests :: TestTree
tests = testGroup "Snapshot tests" [unitTests]

unitTests :: TestTree
unitTests =
    withResource (openFolder "snapshot_tests") closeFolder $ \getPath ->
        testGroup "Unit tests"
            [ testCreateSnapshot getPath
            , testWriteAndSave getPath
            , testSaveRemovesOlderSnapshots getPath
            ]

testCreateSnapshot :: IO FilePath -> TestTree
testCreateSnapshot getPath = testCase "Creates snapshot" $ do
    path <- getPath
    manager <- newSnapshotManager $ Just path
    createSnapshotImpl 100 manager
    doesFileExist "./snapshot_tests/100.partial.snap" >>=
        (@? "File doesn't exist")
    return ()

testWriteAndSave :: IO FilePath -> TestTree
testWriteAndSave getPath = testCase "Writes to snapshot" $ do
    path <- getPath
    manager <- newSnapshotManager $ Just path
    createSnapshotImpl 101 manager
    let snapData = "Sample text"
    writeSnapshotImpl (C.pack snapData) 101 manager
    saveSnapshotImpl 101 manager
    s <- readFile "./snapshot_tests/101.completed.snap"
    s @?= snapData

testSaveRemovesOlderSnapshots :: IO FilePath -> TestTree
testSaveRemovesOlderSnapshots getPath =
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
        doesFileExist "./snapshot_tests/102.partial.snap" >>=
            (@? "File exists") . not
        doesFileExist "./snapshot_tests/103.partial.snap" >>=
            (@? "File exists") . not
        doesFileExist "./snapshot_tests/104.completed.snap" >>=
            (@? "File exists") . not
        doesFileExist "./snapshot_tests/105.completed.snap" >>=
            (@? "File doesn't exist")
        doesFileExist "./snapshot_tests/106.partial.snap" >>=
            (@? "File doesn't exist")
        return ()
