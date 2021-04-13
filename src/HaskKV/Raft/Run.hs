module HaskKV.Raft.Run where

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
import Optics
import Optics.State.Operators

runRaft
  :: ( DebugM m
     , MonadState RaftState m
     , ServerM (RaftMessage (LogEntry k v)) ServerEvent m
     , ApplyEntryM k v (LogEntry k v) m
     , TempLogM (LogEntry k v) m
     , SnapshotM s m
     , LoadSnapshotM s m
     , PersistM m
     )
  => m ()
runRaft = do
  commitIndex' <- guse #commitIndex
  lastApplied' <- guse #lastApplied
  when (commitIndex' > lastApplied') $ do
    #lastApplied %= (+ 1)
    entry <- loadEntry (lastApplied' + 1)
    debug $ "Applying entry: " ++ show entry
    mapM_ applyEntry entry

  use #stateType >>= \case
    Follower    -> runFollower
    Candidate _ -> runCandidate
    Leader    _ -> runLeader
