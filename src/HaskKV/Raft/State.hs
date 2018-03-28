{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Raft.State where

import Control.Lens
import Data.Time

type Time = UTCTime

data StateType
    = Follower
    | Candidate Int
    | Leader
        { _nextIndex  :: [Int]
        , _matchIndex :: [Int]
        }
    deriving (Show, Eq)

data RaftState = RaftState
    { _stateType   :: StateType
    , _currTerm    :: Int
    , _votedFor    :: Int
    , _commitIndex :: Int
    , _lastApplied :: Int
    , _serverID    :: Int
    } deriving (Show, Eq)
makeFieldsNoPrefix ''RaftState
