module HaskKV.Raft.Utils where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Utils

startElection :: ( MonadReader (TVar RaftState) m
                 , MonadIO m
                 , LogM e m
                 , ServerM (RaftMessage e) ServerError m
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
    Just lastEntry <- lastIndex >>= loadEntry
    s <- liftIO $ readTVarIO var
    broadcast $ RequestVote
        { _candidateID = _mainServerID s
        , _term        = _currTerm s
        , _lastLogIdx  = entryIndex lastEntry
        , _lastLogTerm = entryTerm lastEntry
        }
