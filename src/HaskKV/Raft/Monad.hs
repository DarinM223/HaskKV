module HaskKV.Raft.Monad where

import Control.Concurrent.STM
import Control.Monad.State.Strict
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Server
import HaskKV.Store

newtype RaftT s msg k v e m a = RaftT
    { unRaftT :: StateT s
                 (ServerT (msg e)
                 (StoreT k v e m))
                 a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadState s
        , LogM e, TempLogM e, StorageM k v
        , ServerM (msg e) ServerEvent
        )

instance MonadTrans (RaftT s msg k v e) where
    lift = RaftT . lift . lift . lift

deriving instance (MonadIO m, KeyClass k, ValueClass v) =>
    ApplyEntryM k v (LogEntry k v) (RaftT s msg k v (LogEntry k v) m)

runRaftT :: (MonadIO m)
         => RaftT s msg k v e m a
         -> Store k v e
         -> ServerState (msg e)
         -> s
         -> m (a, s)
runRaftT m store state raft = do
    storeVar <- liftIO $ newTVarIO store
    runRaftTVar m storeVar state raft

runRaftTVar :: RaftT s msg k v e m a
            -> TVar (Store k v e)
            -> ServerState (msg e)
            -> s
            -> m (a, s)
runRaftTVar m store state raft
    = flip runStoreTVar store
    . flip runServerT state
    . flip runStateT raft
    . unRaftT
    $ m

data Params s msg k v e = Params
    { _store       :: TVar (Store k v e)
    , _serverState :: ServerState (msg e)
    , _raftState   :: s
    }

runRaftTParams :: RaftT s msg k v e m a
               -> Params s msg k v e
               -> m (a, s)
runRaftTParams m p =
    runRaftTVar m (_store p) (_serverState p) (_raftState p)
