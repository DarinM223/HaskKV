module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server

startElection :: ( MonadState RaftState m
                 , MonadIO m
                 , LogM e m
                 , ServerM (RaftMessage e) ServerEvent m
                 , Entry e
                 )
              => m ()
startElection = do
    sid <- use serverID
    stateType .= Candidate 1
    currTerm %= (+ 1)
    votedFor .= Just sid

    Just lastEntry <- lastIndex >>= loadEntry
    currTerm' <- use currTerm
    broadcast $ RequestVote
        { _candidateID = sid
        , _term        = currTerm'
        , _lastLogIdx  = entryIndex lastEntry
        , _lastLogTerm = entryTerm lastEntry
        }
