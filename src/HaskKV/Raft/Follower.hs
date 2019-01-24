module HaskKV.Raft.Follower where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

runFollower
  :: ( MonadState RaftState m
     , HasDebugM effs (DebugM m)
     , HasLogM effs (LogM e m)
     , HasServerM effs (ServerM (RaftMessage e) ServerEvent m)
     , HasSnapshotM effs (SnapshotM s m)
     , HasLoadSnapshotM effs (LoadSnapshotM s m)
     , HasPersistM effs (PersistM m)
     , Entry e )
  => effs -> m ()
runFollower effs = recv >>= \case
  Left ElectionTimeout -> do
    debug "Starting election"
    reset ElectionTimeout
    startElection effs
  Left  HeartbeatTimeout     -> reset HeartbeatTimeout
  Right rv@RequestVote{}     -> get >>= handleRequestVote effs rv
  Right ae@AppendEntries{}   -> get >>= handleAppendEntries effs ae
  Right is@InstallSnapshot{} -> get >>= handleInstallSnapshot effs is
  Right (Response _ resp)    -> get >>= handleFollowerResponse persistM' resp
 where
  ServerM { recv, reset } = effs ^. serverM
  DebugM debug = effs ^. debugM
  persistM' = effs ^. persistM

handleFollowerResponse persistM msg@(VoteResponse term _) s
  | term > getField @"_currTerm" s = transitionToFollower persistM msg
  | otherwise                      = return ()

handleFollowerResponse _ _ _ = return ()
