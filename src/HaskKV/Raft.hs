module HaskKV.Raft where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import GHC.Generics
import HaskKV.Log (execLogTVar, Entry, Log, LogM, LogT (..))
import HaskKV.Server (execServerT, ServerState, ServerM, ServerT)
import HaskKV.Store (execStoreTVar, StorageM, Store, StoreT)

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
    { unRaftT :: ReaderT (TVar RaftState)
                 (ServerT (RaftMessage e)
                 (LogT e
                 (StoreT k v m)))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar RaftState)
        , LogM e, StorageM k v, ServerM (RaftMessage e)
        )

execRaftT :: (MonadIO m, Entry e)
          => RaftT k v e m a
          -> Store k v
          -> Log e
          -> ServerState (RaftMessage e)
          -> RaftState
          -> m a
execRaftT m store log state raft = do
    storeVar <- liftIO $ newTVarIO store
    logVar <- liftIO $ newTVarIO log
    raftVar <- liftIO $ newTVarIO raft
    execRaftTVar m storeVar logVar state raftVar

execRaftTVar :: (MonadIO m, Entry e)
             => RaftT k v e m a
             -> TVar (Store k v)
             -> TVar (Log e)
             -> ServerState (RaftMessage e)
             -> TVar RaftState
             -> m a
execRaftTVar m store log state raft
    = flip execStoreTVar store
    . flip execLogTVar log
    . flip execServerT state
    . flip runReaderT raft
    . unRaftT
    $ m

data Params k v e = Params
    { _store       :: TVar (Store k v)
    , _log         :: TVar (Log e)
    , _serverState :: ServerState (RaftMessage e)
    , _raftState   :: TVar RaftState
    }

execRaftTParams :: (MonadIO m, Entry e)
                => RaftT k v e m a
                -> Params k v e
                -> m a
execRaftTParams m p =
    execRaftTVar m (_store p) (_log p) (_serverState p) (_raftState p)
