module HaskKV.Raft.Message where

import Data.Binary
import GHC.Generics
import HaskKV.Types

import qualified Data.ByteString as B

data RaftResponse
    = AppendResponse
        { _term      :: Int
        , _success   :: Bool
        , _lastIndex :: LogIndex
        }
    | VoteResponse
        { _term    :: Int
        , _success :: Bool
        }
    | InstallSnapshotResponse
        { _term :: Int
        }
    deriving (Show, Eq, Generic)

data RaftMessage e
    = RequestVote
        { _candidateID :: SID
        , _term        :: Int
        , _lastLogIdx  :: LogIndex
        , _lastLogTerm :: Int
        }
    | AppendEntries
        { _term        :: Int
        , _leaderId    :: SID
        , _prevLogIdx  :: LogIndex
        , _prevLogTerm :: Int
        , _entries     :: [e]
        , _commitIdx   :: LogIndex
        }
    | InstallSnapshot
        { _term              :: Int
        , _leaderId          :: SID
        , _lastIncludedIndex :: LogIndex
        , _lastIncludedTerm  :: Int
        , _offset            :: Int
        , _data              :: B.ByteString
        , _done              :: Bool
        }
    | Response SID RaftResponse
    deriving (Show, Eq, Generic)

instance Binary RaftResponse
instance (Binary e) => Binary (RaftMessage e)
