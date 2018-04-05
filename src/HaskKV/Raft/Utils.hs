module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server

transitionToFollower :: (MonadState RaftState m, HasField "_term" msg Int)
                     => msg
                     -> m ()
transitionToFollower msg = do
    stateType .= Follower
    currTerm .= getField @"_term" msg
    return ()

transitionToLeader msg = do
    reset HeartbeatTimeout
    -- TODO(DarinM223): implement this
    undefined

quorumSize :: (ServerM msg ServerEvent m) => m Int
quorumSize = do
    servers <- numServers
    return $ servers `quot` 2 + 1

startElection :: ( MonadState RaftState m
                 , MonadIO m
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
