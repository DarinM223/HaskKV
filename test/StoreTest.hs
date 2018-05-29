module StoreTest (tests) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.IO.Class
import Data.Time
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Store
import HaskKV.Log
import HaskKV.Log.Entry

tests :: TestTree
tests = testGroup "Store tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "Gets and sets value" $ do
        store <- newTVarIO emptyStore'
        v <- getValueImpl 2 store
        liftIO $ v @?= Nothing
        v' <- liftIO $ createStoreValue 2 1 "value"
        setValueImpl 2 v' store
        v'' <- getValueImpl 2 store
        liftIO $ v'' @?= Just v'
    , testCase "replaceStore only replaces if CAS values match" $ do
        store <- newTVarIO emptyStore'
        v <- liftIO $ createStoreValue 2 1 "value"
        diffCAS <- liftIO $ createStoreValue 2 2 "don't set me"
        sameCAS <- liftIO $ createStoreValue 2 1 "changed"
        setValueImpl 2 v store
        result <- replaceValueImpl 2 diffCAS store
        liftIO $ result @?= Nothing
        getValueImpl 2 store >>= liftIO . (@=? Just v)
        result' <- replaceValueImpl 2 sameCAS store
        liftIO $ result' @?= Just 2
        let expectedCAS = sameCAS { _version = 2 }
        getValueImpl 2 store >>= liftIO . (@=? Just expectedCAS)
    , testCase "cleanupExpired removes expired values" $ do
        store <- newTVarIO emptyStore'
        expire1Sec <- liftIO $ createStoreValue 1 1 "value1"
        expire2Sec <- liftIO $ createStoreValue 2 1 "value2"
        expire2Sec' <- liftIO $ createStoreValue 2 1 "value3"
        expire6Sec <- liftIO $ createStoreValue 6 1 "value4"
        forM_ (zip [1..] [expire1Sec, expire2Sec, expire2Sec', expire6Sec]) $ \(k, v) ->
            setValueImpl k v store
        liftIO $ threadDelay 2100000
        liftIO getCurrentTime >>= flip cleanupExpiredImpl store
        getValueImpl 1 store >>= liftIO . (@=? Nothing)
        getValueImpl 2 store >>= liftIO . (@=? Nothing)
        getValueImpl 3 store >>= liftIO . (@=? Nothing)
        getValueImpl 4 store >>= liftIO . (@=? Just expire6Sec)
    ]
  where
    emptyStore' = emptyStore :: Store Int (StoreValue String) (LogEntry Int (StoreValue String))
