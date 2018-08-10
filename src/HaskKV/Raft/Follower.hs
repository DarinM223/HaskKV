module HaskKV.Raft.Follower where

import Control.Monad.State
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Raft.Debug
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

runFollower :: ( DebugM m
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
    Left HeartbeatTimeout      -> reset HeartbeatTimeout
    Right rv@RequestVote{}     -> get >>= handleRequestVote rv
    Right ae@AppendEntries{}   -> get >>= handleAppendEntries ae
    Right is@InstallSnapshot{} -> get >>= handleInstallSnapshot is
    Right (Response _ resp)    -> get >>= handleFollowerResponse resp

handleFollowerResponse msg@(VoteResponse term _) s
    | term > getField @"_currTerm" s = transitionToFollower msg
    | otherwise                      = return ()

handleFollowerResponse _ _ = return ()
