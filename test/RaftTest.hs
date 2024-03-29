{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE OverloadedLabels #-}
module RaftTest (tests) where

import Control.Monad
import Data.Foldable (for_, traverse_)
import Data.List (nub)
import Data.Maybe
import Data.Traversable (for)
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.Message
import HaskKV.Raft.Run
import HaskKV.Raft.State
import HaskKV.Store.All
import HaskKV.Types
import Mock
import Mock.Instances
import Optics
import Optics.State.Operators
import Test.Tasty
import Test.Tasty.HUnit

import qualified Data.IntMap as IM

tests :: TestTree
tests = testGroup "Raft tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup
  "Unit tests"
  [ testElection
  , testSplitElection
  , testLeaderSendsAppendEntries
  , testLeaderDecrementsMatch
  , testLeaderSendsSnapshot
  ]

initLogEntries :: StoreValue Int -> [LogEntry Int (StoreValue Int)]
initLogEntries value = [entry1, entry2]
 where
  entry1 = LogEntry
    { term      = 0
    , index     = 1
    , entryData = Change (TID 0) 1 value
    , completed = Completed Nothing
    }
  entry2 = LogEntry
    { term      = 0
    , index     = 2
    , entryData = Delete (TID 0) 1
    , completed = Completed Nothing
    }

addedLogEntries :: StoreValue Int -> [LogEntry Int (StoreValue Int)]
addedLogEntries value = [entry1, entry2]
 where
  entry1 = LogEntry
    { term      = 1
    , index     = 1
    , entryData = Change (TID 0) 2 value
    , completed = Completed Nothing
    }
  entry2 = LogEntry
    { term      = 1
    , index     = 2
    , entryData = Change (TID 0) 3 value
    , completed = Completed Nothing
    }

noop :: LogEntry Int (StoreValue Int)
noop = LogEntry { term      = 1
                , index     = 1
                , entryData = Noop
                , completed = Completed Nothing }

testElection :: TestTree
testElection = testCase "Tests normal majority election" $ do
  value <- newStoreValue 2 1 10
  let
    servers            = setupServers [1, 2, 3, 4]
    entries            = initLogEntries value
    ((msgs, state), _) = flip runMockT servers $ do
      runServer 2 $ MockT $ do
        storeEntries entries
        #electionTimer .= True
      runServer 3 $ storeEntries entries
      runServer 4 $ storeEntries entries

      runServers
      msgs <- MockT $ preuse (ix 2 % #receivingMsgs)

      replicateM_ 3 runServers
      state <- MockT $ preuse (ix 2 % #raftState % #stateType)
      return (msgs, state)
  let resp = VoteResponse {raftResponseTerm = 1, raftResponseSuccess = True}
  msgs @?= Just [Response 1 resp, Response 3 resp, Response 4 resp]
  let
    expectedState = LeaderState
      { leaderStateNextIndex  = IM.fromList [(1, 4), (2, 4), (3, 4), (4, 4)]
      , leaderStateMatchIndex = IM.fromList [(1, 0), (2, 0), (3, 0), (4, 0)]
      }
  state @?= (Just $ Leader expectedState)

testSplitElection :: TestTree
testSplitElection = testCase "Test split election" $ do
  let
    servers     = setupServers [1, 2, 3, 4, 5]
    (result, _) = flip runMockT servers $ do
      runServer 2 $ MockT (#electionTimer .= True) >> runRaft
      flushMessages 2
      runServer 3 $ MockT (#electionTimer .= True) >> runRaft
      flushMessages 3

      -- Server 1 sends vote to Server 2, Server 4 sends vote to
      -- Server 3, Server 5 doesn't send any vote.
      replicateM_ 2 (dropMessage 5)
      runServer 4 $ MockT (#receivingMsgs %= reverse) >> runRaft
      flushMessages 4
      runServer 1 runRaft
      flushMessages 1

      replicateM_ 4 runServers
      -- Check that no server has been elected leader.
      state2 <- MockT $ preuse (ix 2 % #raftState % #stateType)
      state3 <- MockT $ preuse (ix 3 % #raftState % #stateType)

      -- Reset timer for server 2 and check that it gets elected.
      runServer 2 $ MockT $ #electionTimer .= True
      replicateM_ 4 runServers
      state2' <- MockT $ preuse (ix 2 % #raftState % #stateType)
      state3' <- MockT $ preuse (ix 3 % #raftState % #stateType)
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
testLeaderSendsAppendEntries
  = let
      text =
        "Test leader sends append entries on election and "
          ++ "follower log is replaced with entries"
      servers = setupServers [1, 2, 3, 4, 5]
    in testCase text $ do
      value <- newStoreValue 2 1 10
      let
        entries     = initLogEntries value
        added       = addedLogEntries value
        (result, _) = flip runMockT servers $ do
          -- Add log entries to minority of servers
          -- so that leader won't have these entries.
          runServer 2 $ storeEntries entries
          runServer 3 $ storeEntries entries
          runServer 1 $ MockT (#electionTimer .= True)
          replicateM_ 4 runServers

          -- Check that the leader broadcasts the correct
          -- AppendEntries messages
          runServer 1 runRaft
          flushMessages 1
          msgs <- for [2, 3, 4, 5] $ \i -> do
            msgs <- MockT $ preuse $ ix i % #receivingMsgs
            runServer i runRaft
            flushMessages i
            return msgs

          -- Add new log entries to leader
          runServer 1 $ do
            MockT $ do
              traverse_ addTemporaryEntry added
              #heartbeatTimer .= True
            runRaft
          flushMessages 1

          -- Check that leader replicates the entries
          -- to the other servers.
          --
          -- The entries added at the beginning should
          -- have been replaced.
          stores <- for [2, 3, 4, 5] $ \i -> do
            runServer i runRaft
            flushMessages i
            runServer i $ MockT $ use #store

          -- Check that matchIndex, nextIndex, and commitIndex
          -- are updated after leader receives responses.
          replicateM_ 8 runServers
          leaderState <- runServer 1 $ MockT $ preuse
            (#raftState % #stateType % _Leader)
          commitIdx <- runServer 1 $ MockT $ use (#raftState % #commitIndex)
          return (msgs, catMaybes stores, join leaderState, commitIdx)
        (msgs, stores, state, commitIdx) = result
        blankAE                          = AppendEntries $ AppendEntries'
          { aeTerm        = 1
          , aeLeaderId    = 1
          , aePrevLogIdx  = 0
          , aePrevLogTerm = 0
          , aeEntries     = [noop]
          , aeCommitIdx   = 0
          }
        expectedEntries = fmap (Just . (: [])) . replicate 4 $ blankAE
      msgs @?= expectedEntries
      let numUnique = length . nub . fmap (show . removeSID) $ stores
      numUnique @?= 1
      let
        storeEntries = (head stores) ^. #log % #entries
        expected =
          IM.fromList
            . fmap (\(i, e) -> (i, setEntryIndex (LogIndex i) e))
            . zip [1 ..]
            $ noop
            : added
      storeEntries @?= expected
      let
        nextIndex  = [(1, 2), (2, 4), (3, 4), (4, 4), (5, 4)]
        matchIndex = [(1, 3), (2, 3), (3, 3), (4, 3), (5, 3)]
      state @?= Just LeaderState
        { leaderStateNextIndex  = IM.fromList nextIndex
        , leaderStateMatchIndex = IM.fromList matchIndex
        }
      commitIdx @?= Just 3

testLeaderDecrementsMatch :: TestTree
testLeaderDecrementsMatch = testCase "Leader decrements match on fail" $ do
  let servers = setupServers [1, 2, 3, 4, 5]
  value <- newStoreValue 2 1 10
  let
    entries     = initLogEntries value
    (result, _) = flip runMockT servers $ do
      runServer 1 $ MockT $ do
        storeEntries entries
        #electionTimer .= True
      runServer 3 $ storeEntries entries
      runServer 4 $ storeEntries entries
      replicateM_ 3 runServers

      msgs <- MockT $ preuse $ ix 1 % #receivingMsgs
      replicateM_ 6 runServers
      state1 <- MockT $ preuse $ ix 1 % #raftState % #stateType % _Leader

      runServer 1 $ MockT $ #heartbeatTimer .= True
      replicateM_ 5 runServers
      state2 <- MockT $ preuse $ ix 1 % #raftState % #stateType % _Leader

      runServer 1 $ MockT $ #heartbeatTimer .= True
      replicateM_ 5 runServers
      state3 <- MockT $ preuse $ ix 1 % #raftState % #stateType % _Leader

      runServer 1 $ MockT $ #heartbeatTimer .= True
      replicateM_ 5 runServers
      state4 <- MockT $ preuse $ ix 1 % #raftState % #stateType % _Leader

      return (msgs, state1, state2, state3, state4)
    (msgs, s1, s2, s3, s4) = result
    msgs' = fromJust msgs
    failResp = AppendResponse { raftResponseTerm = 1
                              , raftResponseSuccess = False
                              , raftResponseLastIndex = 0 }
  elem (Response 2 failResp) msgs'
    && elem (Response 5 failResp) msgs'
    @? "Servers 2 and 5 didn't respond with failure"
  s1 @?= Just LeaderState
    { leaderStateNextIndex  = IM.fromList [(1, 4), (2, 3), (3, 4), (4, 4), (5, 3)]
    , leaderStateMatchIndex = IM.fromList [(1, 0), (2, 0), (3, 3), (4, 3), (5, 0)]
    }
  s2 @?= Just LeaderState
    { leaderStateNextIndex  = IM.fromList [(1, 4), (2, 2), (3, 4), (4, 4), (5, 2)]
    , leaderStateMatchIndex = IM.fromList [(1, 3), (2, 0), (3, 3), (4, 3), (5, 0)]
    }
  s3 @?= Just LeaderState
    { leaderStateNextIndex  = IM.fromList [(1, 4), (2, 1), (3, 4), (4, 4), (5, 1)]
    , leaderStateMatchIndex = IM.fromList [(1, 3), (2, 0), (3, 3), (4, 3), (5, 0)]
    }
  s4 @?= Just LeaderState
    { leaderStateNextIndex  = IM.fromList [(1, 4), (2, 4), (3, 4), (4, 4), (5, 4)]
    , leaderStateMatchIndex = IM.fromList [(1, 3), (2, 3), (3, 3), (4, 3), (5, 3)]
    }

testLeaderSendsSnapshot :: TestTree
testLeaderSendsSnapshot = testCase "Leader sends snapshot" $ do
  let servers = setupServers [1, 2, 3, 4, 5]
  value <- newStoreValue 2 1 10
  let
    entries     = addedLogEntries value
    (result, _) = flip runMockT servers $ do
      runServer 1 $ MockT $ do
        storeEntries entries
        #electionTimer .= True
      traverse_ (flip runServer (storeEntries entries)) [2, 3, 4]
      replicateM_ 5 runServers

      runServer 1 $ do
        MockT $ #heartbeatTimer .= True
        runRaft
      flushMessages 1
      for_ [2, 3, 4] $ \i -> do
        runServer i runRaft
        flushMessages i
      dropMessage 5
      replicateM_ 7 runServers

      runServer 1 $ do
        takeSnapshot
        MockT $ #heartbeatTimer .= True
      replicateM_ 15 runServers

      store1 <- MockT $ preuse $ ix 1 % #store
      store5 <- MockT $ preuse $ ix 5 % #store

      runServer 1 $ MockT $ #heartbeatTimer .= True
      runServers
      msgs <- MockT $ preuse $ ix 1 % #receivingMsgs
      return (store1, store5, msgs)
    (store1, store5, msgs) = result
  fmap (show . removeSID) store1 @?= fmap (show . removeSID) store5
  let
    numSnapResponses = length . filter isSnapshotResponse . fromMaybe [] $ msgs
  numSnapResponses @?= 0
 where
  isSnapshotResponse (Response _ (InstallSnapshotResponse _)) = True
  isSnapshotResponse _ = False

removeSID :: StoreData k v e -> StoreData k v e
removeSID store = store { sid = SID 0 }
