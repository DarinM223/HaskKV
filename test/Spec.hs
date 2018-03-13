import Test.Tasty

import qualified StoreTest as Store

tests :: TestTree
tests = testGroup "Tests"
    [ Store.tests
    ]

main = defaultMain tests
