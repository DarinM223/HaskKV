module StoreTest (tests) where

import Control.Concurrent
import Control.Monad
import Data.Time
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Log.Entry
import HaskKV.Store
import HaskKV.Types

tests :: TestTree
tests = testGroup "Store tests" [unitTests]

createStore :: IO (Store Int
                         (StoreValue String)
                         (LogEntry Int (StoreValue String)))
createStore = newStore (SID 0)

unitTests :: TestTree
unitTests = testGroup "UnitTests"
    [ testGetSet
    , testReplace
    , testCleanupExpired
    ]

testGetSet :: TestTree
testGetSet = testCase "Gets and sets value" $ do
    store <- createStore
    getValueImpl 2 store >>= (@=? Nothing)
    v <- newStoreValue 2 1 "value"
    setValueImpl 2 v store
    getValueImpl 2 store >>= (@=? Just v)

testReplace :: TestTree
testReplace = testCase "replaceStore only replaces if CAS values match" $ do
    store <- createStore
    v <- newStoreValue 2 1 "value"
    diffCAS <- newStoreValue 2 2 "don't set me"
    sameCAS <- newStoreValue 2 1 "changed"
    setValueImpl 2 v store
    replaceValueImpl 2 diffCAS store >>= (@=? Nothing)
    getValueImpl 2 store >>= (@=? Just v)
    replaceValueImpl 2 sameCAS store >>= (@=? Just 2)
    let expectedCAS = sameCAS { _version = 2 }
    getValueImpl 2 store >>= (@=? Just expectedCAS)

testCleanupExpired :: TestTree
testCleanupExpired = testCase "cleanupExpired removes expired values" $ do
    store <- createStore
    expire1Sec <- newStoreValue 1 1 "value1"
    expire2Sec <- newStoreValue 2 1 "value2"
    expire2Sec' <- newStoreValue 2 1 "value3"
    expire6Sec <- newStoreValue 6 1 "value4"
    let vals = [expire1Sec, expire2Sec, expire2Sec', expire6Sec]
    forM_ (zip [1..] vals) $ \(k, v) -> setValueImpl k v store
    threadDelay 2100000
    getCurrentTime >>= flip cleanupExpiredImpl store
    getValueImpl 1 store >>= (@=? Nothing)
    getValueImpl 2 store >>= (@=? Nothing)
    getValueImpl 3 store >>= (@=? Nothing)
    getValueImpl 4 store >>= (@=? Just expire6Sec)
