module TempLogTest (tests) where

import Control.Monad
import HaskKV.Log.Entry
import HaskKV.Log.Temp
import HaskKV.Store.All
import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Temp Log tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests" [testAddEntries]

testAddEntries :: TestTree
testAddEntries = testCase "addTemporaryEntry adds entries to _tempEntries" $ do
    tempLog <- newTempLog :: IO (TempLog (LogEntry Int (StoreValue Int)))
    let createEntry n = do
            v <- newStoreValue 10 0 n
            return LogEntry
                { _term = 0
                , _index = 0
                , _data = Change (TID 0) 1 v
                , _completed = Completed Nothing
                }
        unChange (Change _ _ v) = v
        unChange _              = undefined

    -- Tests order of entries.
    mapM_ (flip addTemporaryEntryImpl tempLog <=< createEntry) [1..3]
    entries <- temporaryEntriesImpl tempLog
    fmap (_value . unChange . _data) entries @?= [1, 2, 3]

    -- Tests that entries are cleared after getting temporary entries.
    entries <- temporaryEntriesImpl tempLog
    length entries @?= 0

    -- Tests entries bounded to maxTempEntries
    forM_ [1..maxTempEntries + 500] $ \n -> do
        entry <- createEntry n
        addTemporaryEntryImpl entry tempLog
    entries <- temporaryEntriesImpl tempLog
    length entries @?= maxTempEntries
