module HaskKV.Raft.Follower where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server

runFollower :: ( MonadIO m
               , MonadReader (TVar RaftState) m
               , LogM e m
               , ServerM (RaftMessage e) ServerError m
               , Entry e
               , Eq e
               )
            => m ()
runFollower = do
    msg <- recv
    case msg of
        Left Timeout -> startElection
        _            -> return ()
