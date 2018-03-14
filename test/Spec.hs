import Test.Tasty

import qualified StoreTest as Store
import qualified ServerTest as Server

tests :: TestTree
tests = testGroup "Tests"
    [ Store.tests
    , Server.tests
    ]

main = defaultMain tests
