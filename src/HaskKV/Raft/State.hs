module HaskKV.Raft.State where

data RaftState = RaftState
    { _currTerm :: Int
    , _votedFor :: Int
    }
