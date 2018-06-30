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

data RaftMessage e
    = RequestVote
        { _candidateID :: SID
        , _term        :: LogTerm
        , _lastLogIdx  :: LogIndex
        , _lastLogTerm :: LogTerm
        }
    | AppendEntries
        { _term        :: LogTerm
        , _leaderId    :: SID
        , _prevLogIdx  :: LogIndex
        , _prevLogTerm :: LogTerm
        , _entries     :: [e]
        , _commitIdx   :: LogIndex
        }
    | InstallSnapshot
        { _term              :: LogTerm
        , _leaderId          :: SID
        , _lastIncludedIndex :: LogIndex
        , _lastIncludedTerm  :: LogTerm
        , _offset            :: Int
        , _data              :: B.ByteString
        , _done              :: Bool
        }
    | Response SID RaftResponse
    deriving (Show, Eq, Generic)

instance Binary RaftResponse
instance (Binary e) => Binary (RaftMessage e)
