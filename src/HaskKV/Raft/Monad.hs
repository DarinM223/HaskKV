module HaskKV.Raft.Monad where

import Control.Concurrent.STM
import Control.Monad.State.Strict
import HaskKV.Log (LogM)
import HaskKV.Server (runServerT, ServerEvent, ServerState, ServerM, ServerT)
import HaskKV.Store (runStoreTVar, ApplyEntryM, StorageM, Store, StoreT)

newtype RaftT s msg k v e m a = RaftT
    { unRaftT :: StateT s
                 (ServerT (msg e)
                 (StoreT k v e m))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadState s
        , LogM e, StorageM k v, ApplyEntryM k v e, ServerM (msg e) ServerEvent
        )

runRaftT :: (MonadIO m)
         => RaftT s msg k v e m a
         -> Store k v e
         -> ServerState (msg e)
         -> s
         -> m a
runRaftT m store state raft = do
    storeVar <- liftIO $ newTVarIO store
    runRaftTVar m storeVar state raft

runRaftTVar :: (MonadIO m)
            => RaftT s msg k v e m a
            -> TVar (Store k v e)
            -> ServerState (msg e)
            -> s
            -> m a
runRaftTVar m store state raft
    = flip runStoreTVar store
    . flip runServerT state
    . fmap fst
    . flip runStateT raft
    . unRaftT
    $ m

data Params s msg k v e = Params
    { _store       :: TVar (Store k v e)
    , _serverState :: ServerState (msg e)
    , _raftState   :: s
    }

runRaftTParams :: (MonadIO m)
               => RaftT s msg k v e m a
               -> Params s msg k v e
               -> m a
runRaftTParams m p =
    runRaftTVar m (_store p) (_serverState p) (_raftState p)
