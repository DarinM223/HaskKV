module HaskKV.Raft.Utils where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Time
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Utils

timedOut :: (MonadReader (TVar RaftState) m, MonadIO m) => m Bool
timedOut = ask >>= liftIO . readTVarIO >>= \s -> do
    currTime <- liftIO $ getCurrentTime
    let timeoutTime = addUTCTime (timeoutSeconds s) (_lastRecv s)
    return $ diff timeoutTime currTime <= 0
  where
    diff a b = realToFrac (diffUTCTime a b)
    timeoutSeconds = fromRational
                   . toRational
                   . secondsToDiffTime
                   . _timeout

resetTimeout :: (MonadReader (TVar RaftState) m, MonadIO m) => m ()
resetTimeout = do
    var <- ask
    currTime <- liftIO $ getCurrentTime
    liftIO $ flip modifyTVarIO var $ \s -> s { _lastRecv = currTime }

startElection :: ( MonadReader (TVar RaftState) m
                 , MonadIO m
                 , LogM e m
                 , ServerM (RaftMessage e) m
                 , Entry e
                 )
              => m ()
startElection = do
    var <- ask
    liftIO $ flip modifyTVarIO var $ \s -> s
        { _stateType = Candidate 1 -- One vote for itself
        , _currTerm  = _currTerm s + 1
        , _votedFor  = _mainServerID s
        }
    resetTimeout
    Just lastEntry <- lastIndex >>= loadEntry
    s <- liftIO $ readTVarIO var
    broadcast $ RequestVote
        { _candidateID = _mainServerID s
        , _term        = _currTerm s
        , _lastLogIdx  = entryIndex lastEntry
        , _lastLogTerm = entryTerm lastEntry
        }
