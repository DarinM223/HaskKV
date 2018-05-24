module StoreTest (tests) where

import Control.Concurrent
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
    [ testCase "Gets and sets value" $ flip runStoreT emptyStore $ do
        v <- getValue 2
        liftIO $ v @?= Nothing
        v' <- liftIO $ createStoreValue 2 1 "value"
        setValue 2 v'
        v'' <- getValue 2
        liftIO $ v'' @?= Just v'
    , testCase "replaceStore only replaces if CAS values match" $ flip runStoreT emptyStore $ do
        v <- liftIO $ createStoreValue 2 1 "value"
        diffCAS <- liftIO $ createStoreValue 2 2 "don't set me"
        sameCAS <- liftIO $ createStoreValue 2 1 "changed"
        setValue 2 v
        result <- replaceValue 2 diffCAS
        liftIO $ result @?= Nothing
        getValue 2 >>= liftIO . (@=? (Just v))
        result' <- replaceValue 2 sameCAS
        liftIO $ result' @?= Just 2
        let expectedCAS = sameCAS { _version = 2 }
        getValue 2 >>= liftIO . (@=? (Just expectedCAS))
    , testCase "cleanupExpired removes expired values" $ flip runStoreT emptyStore $ do
        expire1Sec <- liftIO $ createStoreValue 1 1 "value1"
        expire2Sec <- liftIO $ createStoreValue 2 1 "value2"
        expire2Sec' <- liftIO $ createStoreValue 2 1 "value3"
        expire6Sec <- liftIO $ createStoreValue 6 1 "value4"
        forM_ (zip [1..] [expire1Sec, expire2Sec, expire2Sec', expire6Sec]) $ \(k, v) ->
            setValue k v
        liftIO $ threadDelay 2100000
        liftIO getCurrentTime >>= cleanupExpired
        getValue 1 >>= liftIO . (@=? Nothing)
        getValue 2 >>= liftIO . (@=? Nothing)
        getValue 3 >>= liftIO . (@=? Nothing)
        getValue 4 >>= liftIO . (@=? (Just expire6Sec))
    , testCase "addTemporaryEntry adds entries to _tempEntries" $ flip runStoreT emptyStore $ do
        let createEntry = \n -> do
                v <- liftIO $ createStoreValue 10 0 n
                return LogEntry
                    { _term = 0
                    , _index = 0
                    , _data = Change (TID 0) 1 v
                    , _completed = Completed Nothing
                    }
            unChange (Change _ _ v) = v
            unChange _              = undefined

        -- Tests order of entries.
        mapM_ (addTemporaryEntry <=< createEntry) [1..3]
        entries <- temporaryEntries
        liftIO $ fmap (_value . unChange . _data) entries @?= [1, 2, 3]

        -- Tests that entries are cleared after getting temporary entries.
        entries <- temporaryEntries
        liftIO $ length entries @?= 0

        -- Tests entries bounded to maxTempEntries
        mapM_ (addTemporaryEntry <=< createEntry) [1..maxTempEntries + 500]
        entries <- temporaryEntries
        liftIO $ length entries @?= maxTempEntries
    ]
