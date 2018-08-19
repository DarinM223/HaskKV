module LogTest
  ( tests
  )
where

import Test.Tasty

tests :: TestTree
tests = testGroup "Log tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests" []
