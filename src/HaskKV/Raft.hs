module HaskKV.Raft where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import HaskKV.Log (execLogTVar, Log, LogM, LogT (..))
import HaskKV.Server (execServerT, ServerState, ServerM, ServerT)
import HaskKV.Store (execStoreTVar, StorageM, Store, StoreT)

newtype RaftT s msg k v e m a = RaftT
    { unRaftT :: ReaderT (TVar s)
                 (ServerT (msg e)
                 (LogT e
                 (StoreT k v m)))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar s)
        , LogM e, StorageM k v, ServerM (msg e)
        )

execRaftT :: (MonadIO m, Binary (msg e))
          => RaftT s msg k v e m a
          -> Store k v
          -> Log e
          -> ServerState (msg e)
          -> s
          -> m a
execRaftT m store log state raft = do
    storeVar <- liftIO $ newTVarIO store
    logVar <- liftIO $ newTVarIO log
    raftVar <- liftIO $ newTVarIO raft
    execRaftTVar m storeVar logVar state raftVar

execRaftTVar :: (MonadIO m, Binary (msg e))
             => RaftT s msg k v e m a
             -> TVar (Store k v)
             -> TVar (Log e)
             -> ServerState (msg e)
             -> TVar s
             -> m a
execRaftTVar m store log state raft
    = flip execStoreTVar store
    . flip execLogTVar log
    . flip execServerT state
    . flip runReaderT raft
    . unRaftT
    $ m

data Params s msg k v e = Params
    { _store       :: TVar (Store k v)
    , _log         :: TVar (Log e)
    , _serverState :: ServerState (msg e)
    , _raftState   :: TVar s
    }

execRaftTParams :: (MonadIO m, Binary (msg e))
                => RaftT s msg k v e m a
                -> Params s msg k v e
                -> m a
execRaftTParams m p =
    execRaftTVar m (_store p) (_log p) (_serverState p) (_raftState p)
