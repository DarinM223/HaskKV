{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Raft.Run where

import Control.Monad.State (MonadState, when)
import Data.Foldable (traverse_)
import HaskKV.Log.Class (LogM (loadEntry), TempLogM)
import HaskKV.Log.Entry (LogEntry)
import HaskKV.Raft.Candidate (runCandidate)
import HaskKV.Raft.Class (PersistM, DebugM (..))
import HaskKV.Raft.Follower (runFollower)
import HaskKV.Raft.Leader (runLeader)
import HaskKV.Raft.Message (RaftMessage)
import HaskKV.Raft.State (RaftState, StateType (Leader, Follower, Candidate))
import HaskKV.Server.Types (ServerEvent, ServerM)
import HaskKV.Snapshot.Types (SnapshotM)
import HaskKV.Store.Types (ApplyEntryM (..), LoadSnapshotM)
import Optics (use, guse)
import Optics.State.Operators ((%=))

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
    traverse_ applyEntry entry

  use #stateType >>= \case
    Follower    -> runFollower
    Candidate _ -> runCandidate
    Leader    _ -> runLeader
{-# INLINABLE runRaft #-}