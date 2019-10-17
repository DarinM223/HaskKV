module HaskKV.Raft.Follower where

import Control.Lens
import Control.Monad.State
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
  | term > s ^. currTerm = transitionToFollower msg
  | otherwise            = return ()

handleFollowerResponse _ _ = return ()
