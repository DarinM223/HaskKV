module HaskKV.State where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log (LogM, LogME, LogT (..))
import HaskKV.Serialize (Serializable)
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

instance Serializable (RaftMessage e) where
    -- TODO(DarinM223): implement this

data RaftState = RaftState
    { _currTerm :: Int
    , _votedFor :: Int
    }

newtype ServerT k v e m a = ServerT
    { unServerT :: ReaderT (TVar RaftState) (LogT e (MemStoreT k v m)) a }
    deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar RaftState)
        , LogM, LogME e
        , StorageM, StorageMK k, StorageMKV k v
        )
