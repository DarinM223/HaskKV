module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server

import qualified Data.IntMap as IM

transitionToFollower :: (MonadState RaftState m, HasField "_term" msg Int)
                     => msg
                     -> m ()
transitionToFollower msg = do
    stateType .= Follower
    currTerm .= getField @"_term" msg
    return ()

transitionToLeader :: ( LogM e m
                      , MonadState RaftState m
                      , ServerM (RaftMessage e) ServerEvent m
                      , Entry e
                      , HasField "_term" msg Int
                      )
                   => msg
                   -> m ()
transitionToLeader msg = do
    reset HeartbeatTimeout
    lastEntry <- lastIndex >>= loadEntry
    serverID' <- use serverID

    mapM_ (broadcastAppend serverID' $ getField @"_term" msg) lastEntry
    mapM_ setLeader lastEntry
  where
    broadcastAppend sid term entry = broadcast $ AppendEntries
        { _term        = term
        , _leaderId    = sid
        , _prevLogIdx  = entryIndex entry
        , _prevLogTerm = entryTerm entry
        , _entries     = []
        , _commitIdx   = 0
        }
    setLeader entry = do
        ids <- serverIds
        let initNextIndex = entryIndex entry + 1
            nextIndexes   = IM.fromList . fmap (flip (,) initNextIndex) $ ids
            matchIndexes  = IM.fromList . fmap (flip (,) 0) $ ids
        stateType .= Leader { _nextIndex  = nextIndexes
                            , _matchIndex = matchIndexes
                            }
        return ()

quorumSize :: (ServerM msg ServerEvent m) => m Int
quorumSize = do
    servers <- length <$> serverIds
    return $ servers `quot` 2 + 1

startElection :: ( MonadState RaftState m
                 , LogM e m
                 , ServerM (RaftMessage e) ServerEvent m
                 , Entry e
                 )
              => m ()
startElection = do
    sid <- use serverID
    stateType .= Candidate 1
    currTerm %= (+ 1)
    votedFor .= Just sid

    lastEntry <- lastIndex >>= loadEntry
    currTerm' <- use currTerm
    mapM_ (broadcastVote sid currTerm') lastEntry
  where
    broadcastVote sid currTerm lastEntry = broadcast $ RequestVote
        { _candidateID = sid
        , _term        = currTerm
        , _lastLogIdx  = entryIndex lastEntry
        , _lastLogTerm = entryTerm lastEntry
        }
