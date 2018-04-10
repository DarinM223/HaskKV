{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Raft.State where

import Control.Lens
import Data.Time

import qualified Data.IntMap as IM

type Time = UTCTime

data StateType
    = Follower
    | Candidate Int
    | Leader
        { _nextIndex  :: IM.IntMap Int
        , _matchIndex :: IM.IntMap Int
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
