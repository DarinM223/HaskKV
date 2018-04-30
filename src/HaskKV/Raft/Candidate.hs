module HaskKV.Raft.Candidate where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server

runCandidate :: ( MonadIO m
                , MonadState RaftState m
                , LogM e m
                , ServerM (RaftMessage e) ServerEvent m
                , Entry e
                )
             => m ()
runCandidate = do
    msg <- recv
    case msg of
        Left ElectionTimeout -> do
            reset ElectionTimeout
            startElection
        Left HeartbeatTimeout    -> reset HeartbeatTimeout
        Right rv@RequestVote{}   -> get >>= handleRequestVote rv
        Right ae@AppendEntries{} -> get >>= handleAppendEntries ae
        Right (Response _ resp)  -> get >>= handleCandidateResponse resp

handleCandidateResponse msg@(VoteResponse term success) s
    | term > _currTerm s = transitionToFollower msg
    | success == True = do
        stateType._Candidate %= (+ 1)
        votes <- fromMaybe 0 <$> preuse (stateType._Candidate)
        quorumSize' <- quorumSize
        when (votes >= quorumSize') $ transitionToLeader msg

handleCandidateResponse _ _ = return ()
