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
     , HasDebugM effs (DebugM m)
     , HasLogM effs (LogM (LogEntry k v) m)
     , HasServerM effs (ServerM (RaftMessage (LogEntry k v)) ServerEvent m)
     , HasSnapshotM effs (SnapshotM s m)
     , HasLoadSnapshotM effs (LoadSnapshotM s m)
     , HasPersistM effs (PersistM m)
     , KeyClass k, ValueClass v)
  => effs -> m ()
runCandidate effs = recv >>= \case
  Left ElectionTimeout -> do
    debug "Restarting election"
    reset ElectionTimeout
    startElection effs
  Left  HeartbeatTimeout     -> reset HeartbeatTimeout
  Right rv@RequestVote{}     -> get >>= handleRequestVote effs rv
  Right ae@AppendEntries{}   -> get >>= handleAppendEntries effs ae
  Right is@InstallSnapshot{} -> get >>= handleInstallSnapshot effs is
  Right (Response _ resp)    -> get >>= handleCandidateResponse effs resp
 where
  ServerM { recv, reset } = effs ^. serverM
  DebugM debug = effs ^. debugM

handleCandidateResponse effs msg@(VoteResponse term success) s
  | term > getField @"_currTerm" s = do
    debug "Transitioning to follower"
    transitionToFollower persistM' msg
  | success = do
    stateType . _Candidate %= (+ 1)
    votes <- fromMaybe 0 <$> preuse (stateType . _Candidate)
    debug $ "Received " ++ show votes ++ " votes"
    quorumSize' <- quorumSize serverM'
    when (votes >= quorumSize') $ do
      debug "Transitioning to leader"
      transitionToLeader effs msg
 where
  persistM' = effs ^. persistM
  DebugM debug = effs ^. debugM
  serverM' = effs ^. serverM

handleCandidateResponse _ _ _ = return ()
