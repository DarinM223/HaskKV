module HaskKV.Raft.Follower where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Raft.Utils

runFollower :: (MonadReader (TVar RaftState) m, ServerM msg m, Eq msg) => m ()
runFollower = do
    msg <- recv
    when (msg /= Nothing) resetTimeout
    isTimeout <- timedOut
    when isTimeout $ startElection
