module HaskKV.Raft.Candidate where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

runCandidate
  :: ( DebugM m
     , MonadState RaftState m
     , LogM (LogEntry k v) m
     , ServerM (RaftMessage (LogEntry k v)) ServerEvent m
     , StorageM k v m
     , SnapshotM s m
     , LoadSnapshotM s m
     , PersistM m
     )
  => m ()
runCandidate = recv >>= \case
  Left ElectionTimeout -> do
    debug "Restarting election"
    reset ElectionTimeout
    startElection
  Left  HeartbeatTimeout     -> reset HeartbeatTimeout
  Right (RequestVote rv)     -> get >>= handleRequestVote rv
  Right (AppendEntries ae)   -> get >>= handleAppendEntries ae
  Right (InstallSnapshot is) -> get >>= handleInstallSnapshot is
  Right (Response _ resp)    -> get >>= handleCandidateResponse resp

handleCandidateResponse msg@(VoteResponse term success) s
  | term > s^.currTerm = do
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
