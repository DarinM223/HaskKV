module HaskKV.Raft where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log (LogM)
import HaskKV.Server (runServerT, ServerEvent, ServerState, ServerM, ServerT)
import HaskKV.Store (runStoreTVar, StorageM, Store, StoreT)

newtype RaftT s msg k v e m a = RaftT
    { unRaftT :: ReaderT (TVar s)
                 (ServerT (msg e)
                 (StoreT k v e m))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar s)
        , LogM e, StorageM k v, ServerM (msg e) ServerEvent
        )

runRaftT :: (MonadIO m)
         => RaftT s msg k v e m a
         -> Store k v e
         -> ServerState (msg e)
         -> s
         -> m a
runRaftT m store state raft = do
    storeVar <- liftIO $ newTVarIO store
    raftVar <- liftIO $ newTVarIO raft
    runRaftTVar m storeVar state raftVar

runRaftTVar :: (MonadIO m)
            => RaftT s msg k v e m a
            -> TVar (Store k v e)
            -> ServerState (msg e)
            -> TVar s
            -> m a
runRaftTVar m store state raft
    = flip runStoreTVar store
    . flip runServerT state
    . flip runReaderT raft
    . unRaftT
    $ m

data Params s msg k v e = Params
    { _store       :: TVar (Store k v e)
    , _serverState :: ServerState (msg e)
    , _raftState   :: TVar s
    }

runRaftTParams :: (MonadIO m)
               => RaftT s msg k v e m a
               -> Params s msg k v e
               -> m a
runRaftTParams m p =
    runRaftTVar m (_store p) (_serverState p) (_raftState p)
