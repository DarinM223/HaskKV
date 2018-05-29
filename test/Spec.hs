import Test.Tasty

import qualified LogTest as Log
import qualified ServerTest as Server
import qualified StoreTest as Store
import qualified TempLogTest as TempLog

tests :: TestTree
tests = testGroup "Tests"
    [ Store.tests
    , Server.tests
    , Log.tests
    , TempLog.tests
    ]

main = defaultMain tests
