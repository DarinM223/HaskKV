import Test.Tasty

import qualified LogTest as Log
import qualified ServerTest as Server
import qualified StoreTest as Store

tests :: TestTree
tests = testGroup "Tests"
    [ Store.tests
    , Server.tests
    , Log.tests
    ]

main = defaultMain tests
