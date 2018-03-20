module HaskKV.Raft.Utils where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Raft.State

timedOut :: (MonadReader (TVar RaftState) m) => m Bool
timedOut = undefined

resetTimeout :: (MonadReader (TVar RaftState) m) => m ()
resetTimeout = undefined

startElection :: (MonadReader (TVar RaftState) m) => m ()
startElection = undefined
