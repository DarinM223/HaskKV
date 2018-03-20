module LogTest (tests) where

import Test.Tasty
import Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Log tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests" []
