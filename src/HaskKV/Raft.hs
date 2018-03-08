module HaskKV.Raft where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import GHC.Generics
import HaskKV.Log (LogM, LogME, LogT (..))
import HaskKV.Server (ServerM, ServerT)
import HaskKV.Store (StorageM, StorageMK, StorageMKV, MemStoreT)

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
    | Response Int Bool
    deriving (Show, Eq, Generic)

instance (Binary e) => Binary (RaftMessage e)

data RaftState = RaftState
    { _currTerm :: Int
    , _votedFor :: Int
    }

newtype RaftT k v e m a = RaftT
    { unRaftT :: ReaderT (TVar RaftState) (ServerT (RaftMessage e) (LogT e (MemStoreT k v m))) a }
    deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar RaftState)
        , LogM, LogME e
        , StorageM, StorageMK k, StorageMKV k v
        , ServerM (RaftMessage e)
        )
