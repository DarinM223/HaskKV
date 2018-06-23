module RaftTest (tests) where

import Control.Lens
import Control.Monad
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Raft
import HaskKV.Store
import Mock
import Mock.Instances
import Test.Tasty
import Test.Tasty.HUnit

import qualified Data.IntMap as IM

tests :: TestTree
tests = testGroup "Raft tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testElection
    ]

testElection :: TestTree
testElection = testCase "Tests normal majority election" $ do
    value <- newStoreValue 2 1 10
    let servers = setupServers [1, 2, 3, 4]
        entry1 = LogEntry
            { _term      = 1
            , _index     = 1
            , _data      = Change (TID 0) 1 value
            , _completed = Completed Nothing
            }
        entry2 = LogEntry
            { _term      = 1
            , _index     = 2
            , _data      = Delete (TID 0) 1
            , _completed = Completed Nothing
            }
        entries = [entry1, entry2]
        ((msgs, state), _) = flip runMockT servers $ do
            runServer 2 $ MockT $ do
                storeEntries entries
                electionTimer .= True
            runServer 3 $ storeEntries entries
            runServer 4 $ storeEntries entries

            runServers
            msgs <- MockT $ preuse (ix 2 . receivingMsgs)

            replicateM_ 3 runServers
            state <- MockT $ preuse (ix 2 . raftState . stateType)
            return (msgs, state)
    let resp = VoteResponse { _term = 1, _success = True }
    msgs @?= Just [Response 1 resp, Response 3 resp, Response 4 resp]
    let expectedState = LeaderState
            { _nextIndex  = IM.fromList [(1, 3), (2, 3), (3, 3), (4, 3)]
            , _matchIndex = IM.fromList [(1, 0), (2, 0), (3, 0), (4, 0)]
            }
    state @?= (Just $ Leader expectedState)
