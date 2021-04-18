module TempLogTest
  ( tests
  )
where

import Control.Monad ((<=<))
import Data.Foldable (for_, traverse_)
import HaskKV.Log.Entry
import HaskKV.Log.Temp
import HaskKV.Store.All
import Optics
import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Temp Log tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests" [testAddEntries]

testAddEntries :: TestTree
testAddEntries = testCase "addTemporaryEntry adds entries to _tempEntries" $ do
  tempLog <- newTempLog :: IO (TempLog (LogEntry Int (StoreValue Int)))
  let
    createEntry n = do
      v <- newStoreValue 10 0 n
      return LogEntry
        { term      = 0
        , index     = 0
        , entryData = Change (TID 0) 1 v
        , completed = Completed Nothing
        }
    unChange (Change _ _ v) = v
    unChange _              = undefined

  -- Tests order of entries.
  traverse_ (flip addTemporaryEntry' tempLog <=< createEntry) [1 .. 3]
  entries <- temporaryEntries' tempLog
  fmap (_value . unChange . (^. #entryData)) entries @?= [1, 2, 3]

  -- Tests that entries are cleared after getting temporary entries.
  entries <- temporaryEntries' tempLog
  length entries @?= 0

  -- Tests entries bounded to maxTempEntries
  for_ [1 .. maxTempEntries + 500] $ \n -> do
    entry <- createEntry n
    addTemporaryEntry' entry tempLog
  entries <- temporaryEntries' tempLog
  length entries @?= maxTempEntries
