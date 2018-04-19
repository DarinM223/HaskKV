module HaskKV.Raft.Message where

import Data.Binary
import GHC.Generics

data RaftMessage e
    = RequestVote
        { _candidateID :: Int
        , _term        :: Int
        , _lastLogIdx  :: Int
        , _lastLogTerm :: Int
        }
    | AppendEntries
        { _term        :: Int
        , _leaderId    :: Int
        , _prevLogIdx  :: Int
        , _prevLogTerm :: Int
        , _entries     :: [e]
        , _commitIdx   :: Int
        }
    | Response
        { _message :: RaftMessage e
        , _term    :: Int
        , _success :: Bool
        , _sender  :: Int
        }
    deriving (Show, Eq, Generic)

instance (Binary e) => Binary (RaftMessage e)
