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
makePrisms ''StateType

data RaftState = RaftState
    { _stateType   :: StateType
    , _currTerm    :: Int
    , _votedFor    :: Maybe Int
    , _leader      :: Maybe Int
    , _commitIndex :: Int
    , _lastApplied :: Int
    , _serverID    :: Int
    } deriving (Show, Eq)
makeFieldsNoPrefix ''RaftState
