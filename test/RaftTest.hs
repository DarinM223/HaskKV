module RaftTest (tests) where

import Control.Lens
import Control.Monad
import Data.List
import Data.Maybe
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
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
    , testLeaderSendsAppendEntries
    , testLeaderDecrementsMatch
    ]

initLogEntries :: StoreValue Int -> [LogEntry Int (StoreValue Int)]
initLogEntries value = [entry1, entry2]
  where
    entry1 = LogEntry
        { _term      = 0
        , _index     = 1
        , _data      = Change (TID 0) 1 value
        , _completed = Completed Nothing
        }
    entry2 = LogEntry
        { _term      = 0
        , _index     = 2
        , _data      = Delete (TID 0) 1
        , _completed = Completed Nothing
        }

addedLogEntries :: StoreValue Int -> [LogEntry Int (StoreValue Int)]
addedLogEntries value = [entry1, entry2]
  where
    entry1 = LogEntry
        { _term      = 1
        , _index     = 1
        , _data      = Change (TID 0) 2 value
        , _completed = Completed Nothing
        }
    entry2 = LogEntry
        { _term      = 1
        , _index     = 2
        , _data      = Change (TID 0) 3 value
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

testLeaderSendsAppendEntries :: TestTree
testLeaderSendsAppendEntries =
    let text = "Test leader sends append entries on election and "
            ++ "follower log is replaced with entries"
        servers = setupServers [1, 2, 3, 4, 5]
    in testCase text $ do
        value <- newStoreValue 2 1 10
        let entries = initLogEntries value
            added   = addedLogEntries value
            (result, _) = flip runMockT servers $ do
                -- Add log entries to minority of servers
                -- so that leader won't have these entries.
                runServer 2 $ storeEntries entries
                runServer 3 $ storeEntries entries
                runServer 1 $ MockT (electionTimer .= True)
                replicateM_ 4 runServers

                -- Check that the leader broadcasts the correct
                -- AppendEntries messages
                runServer 1 run
                flushMessages 1
                msgs <- forM [2, 3, 4, 5] $ \i -> do
                    msgs <- MockT $ preuse $ ix i . receivingMsgs
                    runServer i run
                    flushMessages i
                    return msgs

                -- Add new log entries to leader
                runServer 1 $ do
                    MockT $ do
                        mapM_ addTemporaryEntry added
                        heartbeatTimer .= True
                    run
                flushMessages 1

                -- Check that leader replicates the entries
                -- to the other servers.
                --
                -- The entries added at the beginning should
                -- have been replaced.
                stores <- forM [2, 3, 4, 5] $ \i -> do
                    runServer i run
                    flushMessages i
                    runServer i $ MockT $ use store

                -- Check that matchIndex, nextIndex, and commitIndex
                -- are updated after leader receives responses.
                replicateM_ 8 runServers
                leaderState <- runServer 1 $ MockT $
                    preuse (raftState . stateType . _Leader)
                commitIdx <- runServer 1 $ MockT $ use (raftState . commitIndex)
                return (msgs, catMaybes stores, join leaderState, commitIdx)
            (msgs, stores, state, commitIdx) = result
            blankAE = AppendEntries
                { _term        = 1
                , _leaderId    = 1
                , _prevLogIdx  = 0
                , _prevLogTerm = 0
                , _entries     = []
                , _commitIdx   = 0
                }
            expectedEntries = fmap (Just . (: [])) . replicate 4 $ blankAE
        msgs @?= expectedEntries
        let numUnique = length . nub . fmap show $ stores
        numUnique @?= 1
        let storeEntries = getField @"_entries" . _log . head $ stores
        storeEntries @?= IM.fromList (zip [1..] added)
        let nextIndex  = [(1, 1), (2, 3), (3, 3), (4, 3), (5, 3)]
            matchIndex = [(1, 2), (2, 2), (3, 2), (4, 2), (5, 2)]
        state @?= Just LeaderState
            { _nextIndex  = IM.fromList nextIndex
            , _matchIndex = IM.fromList matchIndex
            }
        commitIdx @?= Just 2

testLeaderDecrementsMatch :: TestTree
testLeaderDecrementsMatch = testCase "Leader decements match on fail" $ do
    let servers = setupServers [1, 2, 3, 4, 5]
    value <- newStoreValue 2 1 10
    let entries = initLogEntries value
        (result, _) = flip runMockT servers $ do
            runServer 1 $ MockT $ do
                storeEntries entries
                electionTimer .= True
            runServer 3 $ storeEntries entries
            runServer 4 $ storeEntries entries
            replicateM_ 3 runServers

            msgs <- MockT $ preuse $ ix 1 . receivingMsgs
            replicateM_ 6 runServers
            state1 <- MockT $ preuse $ ix 1 . raftState . stateType . _Leader

            runServer 1 $ MockT $ heartbeatTimer .= True
            replicateM_ 5 runServers
            state2 <- MockT $ preuse $ ix 1 . raftState . stateType . _Leader

            runServer 1 $ MockT $ heartbeatTimer .= True
            replicateM_ 5 runServers
            state3 <- MockT $ preuse $ ix 1 . raftState . stateType . _Leader

            return (msgs, state1, state2, state3)
        (Just msgs, s1, s2, s3) = result
        failResp = AppendResponse { _term      = 1
                                  , _success   = False
                                  , _lastIndex = 0
                                  }
    elem (Response 2 failResp) msgs && elem (Response 5 failResp) msgs @?
        "Servers 2 and 5 didn't respond with failure"
    s1 @?= Just LeaderState
        { _nextIndex  = IM.fromList [(1,3),(2,2),(3,3),(4,3),(5,2)]
        , _matchIndex = IM.fromList [(1,0),(2,0),(3,2),(4,2),(5,0)]
        }
    s2 @?= Just LeaderState
        { _nextIndex  = IM.fromList [(1,3),(2,1),(3,3),(4,3),(5,1)]
        , _matchIndex = IM.fromList [(1,2),(2,0),(3,2),(4,2),(5,0)]
        }
    s3 @?= Just LeaderState
        { _nextIndex  = IM.fromList [(1,3),(2,3),(3,3),(4,3),(5,3)]
        , _matchIndex = IM.fromList [(1,2),(2,2),(3,2),(4,2),(5,2)]
        }
