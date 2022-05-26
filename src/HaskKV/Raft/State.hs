{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Raft.State where

import Data.Binary (Binary)
import Data.Time (UTCTime)
import GHC.Generics (Generic)
import HaskKV.Types (LogIndex, LogTerm, SID (SID))
import Optics ((^.), makeFieldLabels, makePrisms)

import qualified Data.IntMap as IM

type Time = UTCTime

data LeaderState = LeaderState
  { leaderStateNextIndex  :: IM.IntMap LogIndex
  , leaderStateMatchIndex :: IM.IntMap LogIndex
  } deriving (Show, Eq)
makeFieldLabels ''LeaderState

data StateType
  = Follower
  | Candidate Int
  | Leader LeaderState
  deriving (Show, Eq)
makePrisms ''StateType

data RaftState = RaftState
  { raftStateStateType   :: StateType
  , raftStateCurrTerm    :: LogTerm
  , raftStateVotedFor    :: Maybe SID
  , raftStateLeader      :: Maybe SID
  , raftStateCommitIndex :: LogIndex
  , raftStateLastApplied :: LogIndex
  , raftStateServerID    :: SID
  } deriving (Show, Eq)
makeFieldLabels ''RaftState

data PersistentState = PersistentState
  { persistentStateCurrTerm :: LogTerm
  , persistentStateVotedFor :: Maybe SID
  } deriving (Show, Eq, Generic)
makeFieldLabels ''PersistentState

instance Binary PersistentState

persistentStateFilename :: SID -> FilePath
persistentStateFilename (SID sid) = show sid ++ ".state"

newPersistentState :: RaftState -> PersistentState
newPersistentState s = PersistentState
  { persistentStateCurrTerm = s ^. #currTerm
  , persistentStateVotedFor = s ^. #votedFor
  }

newRaftState :: SID -> Maybe PersistentState -> RaftState
newRaftState sid s = RaftState
  { raftStateStateType   = Follower
  , raftStateCurrTerm    = maybe 0 (^. #currTerm) s
  , raftStateVotedFor    = s >>= (^. #votedFor)
  , raftStateLeader      = Nothing
  , raftStateCommitIndex = 0
  , raftStateLastApplied = 0
  , raftStateServerID    = sid
  }
