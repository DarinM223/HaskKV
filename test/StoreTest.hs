module StoreTest (tests) where

import Control.Concurrent
import Data.Foldable (for_)
import Data.Time
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Log.Entry
import HaskKV.Store.All
import HaskKV.Types
import qualified HaskKV.Store.Instances as S

tests :: TestTree
tests = testGroup "Store tests" [unitTests]

createStore
  :: IO (Store Int (StoreValue String) (LogEntry Int (StoreValue String)))
createStore = newStore (SID 0) Nothing

unitTests :: TestTree
unitTests = testGroup "UnitTests" [testGetSet, testReplace, testCleanupExpired]

testGetSet :: TestTree
testGetSet = testCase "Gets and sets value" $ do
  store <- createStore
  S.getValue 2 store >>= (@=? Nothing)
  v <- newStoreValue 2 1 "value"
  S.setValue 2 v store
  S.getValue 2 store >>= (@=? Just v)

testReplace :: TestTree
testReplace = testCase "replaceStore only replaces if CAS values match" $ do
  store   <- createStore
  v       <- newStoreValue 2 1 "value"
  diffCAS <- newStoreValue 2 2 "don't set me"
  sameCAS <- newStoreValue 2 1 "changed"
  S.setValue 2 v store
  S.replaceValue 2 diffCAS store >>= (@=? Nothing)
  S.getValue 2 store >>= (@=? Just v)
  S.replaceValue 2 sameCAS store >>= (@=? Just 2)
  let expectedCAS = sameCAS { _version = 2 }
  S.getValue 2 store >>= (@=? Just expectedCAS)

testCleanupExpired :: TestTree
testCleanupExpired = testCase "cleanupExpired removes expired values" $ do
  store       <- createStore
  expire1Sec  <- newStoreValue 1 1 "value1"
  expire2Sec  <- newStoreValue 2 1 "value2"
  expire2Sec' <- newStoreValue 2 1 "value3"
  expire6Sec  <- newStoreValue 6 1 "value4"
  let vals = [expire1Sec, expire2Sec, expire2Sec', expire6Sec]
  for_ (zip [1 ..] vals) $ \(k, v) -> S.setValue k v store
  threadDelay 2100000
  getCurrentTime >>= flip S.cleanupExpired store
  S.getValue 1 store >>= (@=? Nothing)
  S.getValue 2 store >>= (@=? Nothing)
  S.getValue 3 store >>= (@=? Nothing)
  S.getValue 4 store >>= (@=? Just expire6Sec)
