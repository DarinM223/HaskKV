module HaskKV.Raft.Run where

import Control.Lens
import Control.Monad.State
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Raft.Candidate (runCandidate)
import HaskKV.Raft.Class
import HaskKV.Raft.Follower (runFollower)
import HaskKV.Raft.Leader (runLeader)
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

run
  :: ( MonadState RaftState m
     , HasDebugM m effs
     , HasServerM (RaftMessage (LogEntry k v)) ServerEvent m effs
     , HasApplyEntryM (LogEntry k v) m effs
     , HasTempLogM (LogEntry k v) m effs
     , HasSnapshotM s m effs
     , HasLogM (LogEntry k v) m effs
     , HasLoadSnapshotM s m effs
     , HasPersistM m effs
     , KeyClass k, ValueClass v )
  => effs -> m ()
run effs = do
  commitIndex' <- use commitIndex
  lastApplied' <- use lastApplied
  when (commitIndex' > lastApplied') $ do
    lastApplied %= (+ 1)
    entry <- loadEntry (lastApplied' + 1)
    debug $ "Applying entry: " ++ show entry
    mapM_ applyEntry entry

  use stateType >>= \case
    Follower    -> runFollower effs
    Candidate _ -> runCandidate effs
    Leader    _ -> runLeader effs
 where
  LogM { loadEntry } = getLogM effs
  DebugM debug = getDebugM effs
  ApplyEntryM applyEntry = getApplyEntryM effs
