{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Raft.Follower where

import Control.Monad.State (MonadState (get))
import HaskKV.Log.Class (Entry, LogM)
import HaskKV.Raft.Class (PersistM, DebugM (..))
import HaskKV.Raft.Message (RaftResponse (VoteResponse), RaftMessage(..))
import HaskKV.Raft.RPC
import HaskKV.Raft.State (RaftState)
import HaskKV.Raft.Utils (startElection, transitionToFollower)
import HaskKV.Server.Types (ServerEvent (..), ServerM (reset, recv))
import HaskKV.Snapshot.Types (SnapshotM)
import HaskKV.Store.Types (LoadSnapshotM, StorageM)
import Optics ((^.))

runFollower
  :: ( DebugM m
     , MonadState RaftState m
     , LogM e m
     , ServerM (RaftMessage e) ServerEvent m
     , StorageM k v m
     , SnapshotM s m
     , LoadSnapshotM s m
     , PersistM m
     , Entry e
     )
  => m ()
runFollower = recv >>= \case
  Left ElectionTimeout -> do
    debug "Starting election"
    reset ElectionTimeout
    startElection
  Left  HeartbeatTimeout     -> reset HeartbeatTimeout
  Right (RequestVote rv)     -> get >>= handleRequestVote rv
  Right (AppendEntries ae)   -> get >>= handleAppendEntries ae
  Right (InstallSnapshot is) -> get >>= handleInstallSnapshot is
  Right (Response _ resp)    -> get >>= handleFollowerResponse resp

handleFollowerResponse msg@(VoteResponse term _) s
  | term > s ^. #currTerm = transitionToFollower msg
  | otherwise             = return ()

handleFollowerResponse _ _ = return ()
