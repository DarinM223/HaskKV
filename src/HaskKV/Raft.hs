module HaskKV.Raft where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log (LogM)
import HaskKV.Server (execServerT, ServerError, ServerState, ServerM, ServerT)
import HaskKV.Store (execStoreTVar, StorageM, Store, StoreT)

newtype RaftT s msg k v e m a = RaftT
    { unRaftT :: ReaderT (TVar s)
                 (ServerT (msg e)
                 (StoreT k v e m))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar s)
        , LogM e, StorageM k v, ServerM (msg e) ServerError
        )

execRaftT :: (MonadIO m)
          => RaftT s msg k v e m a
          -> Store k v e
          -> ServerState (msg e)
          -> s
          -> m a
execRaftT m store state raft = do
    storeVar <- liftIO $ newTVarIO store
    raftVar <- liftIO $ newTVarIO raft
    execRaftTVar m storeVar state raftVar

execRaftTVar :: (MonadIO m)
             => RaftT s msg k v e m a
             -> TVar (Store k v e)
             -> ServerState (msg e)
             -> TVar s
             -> m a
execRaftTVar m store state raft
    = flip execStoreTVar store
    . flip execServerT state
    . flip runReaderT raft
    . unRaftT
    $ m

data Params s msg k v e = Params
    { _store       :: TVar (Store k v e)
    , _serverState :: ServerState (msg e)
    , _raftState   :: TVar s
    }

execRaftTParams :: (MonadIO m)
                => RaftT s msg k v e m a
                -> Params s msg k v e
                -> m a
execRaftTParams m p =
    execRaftTVar m (_store p) (_serverState p) (_raftState p)
