module SnapshotTest (tests) where

import HaskKV.Snapshot
import Test.Tasty
import Test.Tasty.HUnit
import System.Directory

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
            ]

testCreateSnapshot :: IO FilePath -> TestTree
testCreateSnapshot getPath = testCase "Creates snapshot" $ do
    path <- getPath
    manager <- newSnapshotManager $ Just path
    createSnapshotImpl 100 manager
    doesFileExist "./snapshot_tests/100.partial.snap" >>=
        (@? "File doesn't exist")
    return ()
