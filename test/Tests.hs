module Tests
  ( allTests
  )
where

import Test.Tasty

import qualified LogTest as Log
import qualified ServerTest as Server
import qualified SnapshotTest as Snapshot
import qualified StoreTest as Store
import qualified TempLogTest as TempLog
import qualified RaftTest as Raft

allTests :: TestTree
allTests = testGroup
  "Tests"
  [ Store.tests
  , Server.tests
  , Log.tests
  , TempLog.tests
  , Snapshot.tests
  , Raft.tests
  ]
