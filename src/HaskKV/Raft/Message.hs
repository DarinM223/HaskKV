module HaskKV.Raft.Message where

import Data.Binary
import GHC.Generics
import HaskKV.Types

import qualified Data.ByteString as B

data RaftResponse
  = AppendResponse
    { _term      :: LogTerm
    , _success   :: Bool
    , _lastIndex :: LogIndex
    }
  | VoteResponse
    { _term    :: LogTerm
    , _success :: Bool
    }
  | InstallSnapshotResponse
    { _term :: LogTerm
    }
  deriving (Show, Eq, Generic)

data RequestVote' = RequestVote'
  { _candidateID :: SID
  , _term        :: LogTerm
  , _lastLogIdx  :: LogIndex
  , _lastLogTerm :: LogTerm
  } deriving (Show, Eq, Generic)

data AppendEntries' e = AppendEntries'
  { _term        :: LogTerm
  , _leaderId    :: SID
  , _prevLogIdx  :: LogIndex
  , _prevLogTerm :: LogTerm
  , _entries     :: [e]
  , _commitIdx   :: LogIndex
  } deriving (Show, Eq, Generic)

data InstallSnapshot' = InstallSnapshot'
  { _term              :: LogTerm
  , _leaderId          :: SID
  , _lastIncludedIndex :: LogIndex
  , _lastIncludedTerm  :: LogTerm
  , _offset            :: FilePos
  , _data              :: B.ByteString
  , _done              :: Bool
  } deriving (Show, Eq, Generic)

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
