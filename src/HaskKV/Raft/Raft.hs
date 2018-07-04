module HaskKV.Raft.Raft where

import Control.Lens
import Control.Monad.State
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Server
import HaskKV.Store
import HaskKV.Raft.Candidate (runCandidate)
import HaskKV.Raft.Debug
import HaskKV.Raft.Follower (runFollower)
import HaskKV.Raft.Leader (runLeader)
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Snapshot

run :: ( DebugM m
       , MonadState RaftState m
       , ServerM (RaftMessage (LogEntry k v)) ServerEvent m
       , ApplyEntryM k v (LogEntry k v) m
       , TempLogM (LogEntry k v) m
       , SnapshotM s m
       , LoadSnapshotM s m
       , PersistM m
       )
    => m ()
run = do
    commitIndex' <- use commitIndex
    lastApplied' <- use lastApplied
    when (commitIndex' > lastApplied') $ do
        lastApplied %= (+ 1)
        entry <- loadEntry (lastApplied' + 1)
        debug $ "Applying entry: " ++ show entry
        mapM_ applyEntry entry

    use stateType >>= \case
        Follower    -> runFollower
        Candidate _ -> runCandidate
        Leader _    -> runLeader
