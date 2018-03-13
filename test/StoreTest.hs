module StoreTest (tests) where

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Store tests" [qcProps, unitTests]

qcProps :: TestTree
qcProps = testGroup "Quickcheck"
    [
    ]

-- TODO(DarinM223): test pure functions and
-- test locking of StoreT with multiple threads
unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "1 + 1 = 2" $
        (1 + 1) @?= 2
    ]
