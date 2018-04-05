module HaskKV.Raft.Follower where

import Control.Monad.State
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server

runFollower :: ( MonadIO m
               , MonadState RaftState m
               , LogM e m
               , ServerM (RaftMessage e) ServerEvent m
               , Entry e
               )
            => m ()
runFollower = do
    msg <- recv
    case msg of
        Left ElectionTimeout -> do
            reset ElectionTimeout
            startElection
        Left HeartbeatTimeout    -> reset HeartbeatTimeout
        Right rv@RequestVote{}   -> get >>= handleRequestVote rv
        Right ae@AppendEntries{} -> get >>= handleAppendEntries ae
        Right resp@Response{}    -> get >>= handleFollowerResponse resp

handleFollowerResponse msg@(Response RequestVote{} term _) s
    | term > _currTerm s = transitionToFollower msg
    | otherwise          = return ()

handleFollowerResponse _ _ = return ()
