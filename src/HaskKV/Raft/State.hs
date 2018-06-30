{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Raft.State where

import Control.Lens
import Data.Time
import HaskKV.Types

import qualified Data.IntMap as IM

type Time = UTCTime

data LeaderState = LeaderState
    { _nextIndex  :: IM.IntMap LogIndex
    , _matchIndex :: IM.IntMap LogIndex
    } deriving (Show, Eq)
makeFieldsNoPrefix ''LeaderState

data StateType
    = Follower
    | Candidate Int
    | Leader LeaderState
    deriving (Show, Eq)
makePrisms ''StateType

data RaftState = RaftState
    { _stateType   :: StateType
    , _currTerm    :: Int
    , _votedFor    :: Maybe SID
    , _leader      :: Maybe SID
    , _commitIndex :: LogIndex
    , _lastApplied :: LogIndex
    , _serverID    :: SID
    } deriving (Show, Eq)
makeFieldsNoPrefix ''RaftState

newRaftState :: SID -> RaftState
newRaftState sid = RaftState
    { _stateType   = Follower
    , _currTerm    = 0
    , _votedFor    = Nothing
    , _leader      = Nothing
    , _commitIndex = 0
    , _lastApplied = 0
    , _serverID    = sid
    }
