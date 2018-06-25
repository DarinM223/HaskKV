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
    , testSplitElection
    ]

initLogEntries :: StoreValue Int -> [LogEntry Int (StoreValue Int)]
initLogEntries value = [entry1, entry2]
  where
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

testElection :: TestTree
testElection = testCase "Tests normal majority election" $ do
    value <- newStoreValue 2 1 10
    let servers = setupServers [1, 2, 3, 4]
        entries = initLogEntries value
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

testSplitElection :: TestTree
testSplitElection = testCase "Test split election" $ do
    let servers = setupServers [1, 2, 3, 4, 5]
        (result, _) = flip runMockT servers $ do
            runServer 2 $ MockT (electionTimer .= True) >> run
            flushMessages 2
            runServer 3 $ MockT (electionTimer .= True) >> run
            flushMessages 3

            -- Server 1 sends vote to Server 2, Server 4 sends vote to
            -- Server 3, Server 5 doesn't send any vote.
            replicateM_ 2 (dropMessage 5)
            runServer 4 $ MockT (receivingMsgs %= reverse) >> run
            flushMessages 4
            runServer 1 run
            flushMessages 1

            replicateM_ 4 runServers
            -- Check that no server has been elected leader.
            state2 <- MockT $ preuse (ix 2 . raftState . stateType)
            state3 <- MockT $ preuse (ix 3 . raftState . stateType)

            -- Reset timer for server 2 and check that it gets elected.
            runServer 2 $ MockT $ electionTimer .= True
            replicateM_ 4 runServers
            state2' <- MockT $ preuse (ix 2 . raftState . stateType)
            state3' <- MockT $ preuse (ix 3 . raftState . stateType)
            return (state2, state3, state2', state3')
        (s2, s3, s2', s3') = result
    isCandidate s2 @? "State 2 not candidate"
    isCandidate s3 @? "State 3 not candidate"
    isLeader s2' @? "State 2 after not leader"
    isFollower s3' @? "State 3 after not follower"
  where
    isFollower (Just Follower) = True
    isFollower _               = False
    isCandidate (Just (Candidate _)) = True
    isCandidate _                    = False
    isLeader (Just (Leader _)) = True
    isLeader _                 = False
