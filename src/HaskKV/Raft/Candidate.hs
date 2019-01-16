module HaskKV.Raft.Candidate where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import GHC.Records
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
  :: ( MonadState RaftState m
     , HasDebugM m effs
     , HasLogM (LogEntry k v) m effs
     , HasServerM (RaftMessage (LogEntry k v)) ServerEvent m effs
     , HasSnapshotM s m effs
     , HasLoadSnapshotM s m effs
     , HasPersistM m effs
     , KeyClass k, ValueClass v)
  => effs -> m ()
runCandidate effs = recv serverM >>= \case
  Left ElectionTimeout -> do
    debug debugM "Restarting election"
    reset serverM ElectionTimeout
    startElection effs
  Left  HeartbeatTimeout     -> reset serverM HeartbeatTimeout
  Right rv@RequestVote{}     -> get >>= handleRequestVote effs rv
  Right ae@AppendEntries{}   -> get >>= handleAppendEntries effs ae
  Right is@InstallSnapshot{} -> get >>= handleInstallSnapshot effs is
  Right (Response _ resp)    -> get >>= handleCandidateResponse effs resp
 where
  serverM = getServerM effs
  debugM = getDebugM effs

handleCandidateResponse effs msg@(VoteResponse term success) s
  | term > getField @"_currTerm" s = do
    debug debugM "Transitioning to follower"
    transitionToFollower persistM msg
  | success = do
    stateType . _Candidate %= (+ 1)
    votes <- fromMaybe 0 <$> preuse (stateType . _Candidate)
    debug debugM $ "Received " ++ show votes ++ " votes"
    quorumSize' <- quorumSize serverM
    when (votes >= quorumSize') $ do
      debug debugM "Transitioning to leader"
      transitionToLeader effs msg
 where
  persistM = getPersistM effs
  debugM = getDebugM effs
  serverM = getServerM effs

handleCandidateResponse _ _ _ = return ()
