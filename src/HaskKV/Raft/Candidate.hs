module HaskKV.Raft.Candidate where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import HaskKV.Log
import HaskKV.Raft.Debug
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

runCandidate :: ( DebugM m
                , MonadState RaftState m
                , LogM e m
                , ServerM (RaftMessage e) ServerEvent m
                , StorageM k v m
                , SnapshotM s m
                , LoadSnapshotM s m
                , Entry e
                )
             => m ()
runCandidate = recv >>= \case
    Left ElectionTimeout -> do
        debug "Restarting election"
        reset ElectionTimeout
        startElection
    Left HeartbeatTimeout      -> reset HeartbeatTimeout
    Right rv@RequestVote{}     -> get >>= handleRequestVote rv
    Right ae@AppendEntries{}   -> get >>= handleAppendEntries ae
    Right is@InstallSnapshot{} -> get >>= handleInstallSnapshot is
    Right (Response _ resp)    -> get >>= handleCandidateResponse resp

handleCandidateResponse msg@(VoteResponse term success) s
    | term > _currTerm s = do
        debug "Transitioning to follower"
        transitionToFollower msg
    | success = do
        stateType._Candidate %= (+ 1)
        votes <- fromMaybe 0 <$> preuse (stateType._Candidate)
        debug $ "Received " ++ show votes ++ " votes"
        quorumSize' <- quorumSize
        when (votes >= quorumSize') $ do
            debug "Transitioning to leader"
            transitionToLeader msg

handleCandidateResponse _ _ = return ()
