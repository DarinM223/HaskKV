module HaskKV.Raft.Raft where

import Control.Lens
import Control.Monad.State
import HaskKV.Log
import HaskKV.Server
import HaskKV.Store
import HaskKV.Raft.Candidate (runCandidate)
import HaskKV.Raft.Follower (runFollower)
import HaskKV.Raft.Leader (runLeader)
import HaskKV.Raft.Message
import HaskKV.Raft.Utils
import HaskKV.Raft.State
import HaskKV.Snapshot

run :: ( MonadIO m
       , MonadState RaftState m
       , ServerM (RaftMessage e) ServerEvent m
       , ApplyEntryM k v e m
       , TempLogM e m
       , Entry e
       , SnapshotM m
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
