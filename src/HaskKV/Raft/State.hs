{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Raft.State where

import Control.Lens
import Data.Binary
import Data.Time
import GHC.Generics
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
  , _currTerm    :: LogTerm
  , _votedFor    :: Maybe SID
  , _leader      :: Maybe SID
  , _commitIndex :: LogIndex
  , _lastApplied :: LogIndex
  , _serverID    :: SID
  } deriving (Show, Eq)
makeFieldsNoPrefix ''RaftState

data PersistentState = PersistentState
  { _currTerm :: LogTerm
  , _votedFor :: Maybe SID
  } deriving (Show, Eq, Generic)
makeFieldsNoPrefix ''PersistentState

instance Binary PersistentState

persistentStateFilename :: SID -> FilePath
persistentStateFilename (SID sid) = show sid ++ ".state"

newPersistentState :: RaftState -> PersistentState
newPersistentState s = PersistentState
  { _currTerm = s^.currTerm
  , _votedFor = s^.votedFor
  }

newRaftState :: SID -> Maybe PersistentState -> RaftState
newRaftState sid s = RaftState
  { _stateType   = Follower
  , _currTerm    = maybe 0 (^. currTerm) s
  , _votedFor    = s >>= (^. votedFor)
  , _leader      = Nothing
  , _commitIndex = 0
  , _lastApplied = 0
  , _serverID    = sid
  }
