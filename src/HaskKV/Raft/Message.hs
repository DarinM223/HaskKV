{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Raft.Message where

import Data.Binary (Binary)
import GHC.Generics (Generic)
import HaskKV.Types (FilePos, LogIndex, LogTerm, SID)
import Optics

import qualified Data.ByteString as B

data RaftResponse
  = AppendResponse
    { raftResponseTerm      :: LogTerm
    , raftResponseSuccess   :: Bool
    , raftResponseLastIndex :: LogIndex
    }
  | VoteResponse
    { raftResponseTerm    :: LogTerm
    , raftResponseSuccess :: Bool
    }
  | InstallSnapshotResponse
    { raftResponseTerm :: LogTerm
    }
  deriving (Show, Eq, Generic)
makeFieldLabels ''RaftResponse

data RequestVote' = RequestVote'
  { rvCandidateID :: SID
  , rvTerm        :: LogTerm
  , rvLastLogIdx  :: LogIndex
  , rvLastLogTerm :: LogTerm
  } deriving (Show, Eq, Generic)
makeFieldLabelsWith
  (fieldLabelsRules & lensField .~ abbreviatedNamer)
  ''RequestVote'

data AppendEntries' e = AppendEntries'
  { aeTerm        :: LogTerm
  , aeLeaderId    :: SID
  , aePrevLogIdx  :: LogIndex
  , aePrevLogTerm :: LogTerm
  , aeEntries     :: [e]
  , aeCommitIdx   :: LogIndex
  } deriving (Show, Eq, Generic)
makeFieldLabelsWith
  (fieldLabelsRules & lensField .~ abbreviatedNamer)
  ''AppendEntries'

data InstallSnapshot' = InstallSnapshot'
  { isTerm              :: LogTerm
  , isLeaderId          :: SID
  , isLastIncludedIndex :: LogIndex
  , isLastIncludedTerm  :: LogTerm
  , isOffset            :: FilePos
  , isData              :: B.ByteString
  , isDone              :: Bool
  } deriving (Show, Eq, Generic)
makeFieldLabelsWith
  (fieldLabelsRules & lensField .~ abbreviatedNamer)
  ''InstallSnapshot'

data RaftMessage e
  = RequestVote RequestVote'
  | AppendEntries (AppendEntries' e)
  | InstallSnapshot InstallSnapshot'
  | Response SID RaftResponse
  deriving (Show, Eq, Generic)

instance Binary RaftResponse
instance Binary RequestVote'
instance (Binary e) => Binary (AppendEntries' e)
instance Binary InstallSnapshot'
instance (Binary e) => Binary (RaftMessage e)
