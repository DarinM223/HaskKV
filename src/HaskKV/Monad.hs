{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Monad where

import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Deriving.Via
import Data.Proxy
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.Temp
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

data AppConfig msg k v e = AppConfig
    { _store       :: TVar (Store k v e)
    , _tempLog     :: TempLog e
    , _serverState :: ServerState msg
    , _isLeader    :: TVar Bool
    , _snapManager :: SnapshotManager
    }

instance HasServerState msg (AppConfig msg k v e) where
    getServerState = _serverState
instance HasStoreTVar k v e (AppConfig msg k v e) where
    getStoreTVar = _store
instance HasTempLog e (AppConfig msg k v e) where
    getTempLog = _tempLog
instance HasSnapshotManager (AppConfig msg k v e) where
    getSnapshotManager = _snapManager

newAppConfig :: Maybe FilePath -> ServerState msg -> IO (AppConfig msg k v e)
newAppConfig snapshotDirectory serverState = do
    isLeader <- newTVarIO False
    store <- newTVarIO emptyStore
    tempLog <- newTempLog
    snapManager <- newSnapshotManager snapshotDirectory
    return AppConfig
        { _store       = store
        , _tempLog     = tempLog
        , _serverState = serverState
        , _isLeader    = isLeader
        , _snapManager = snapManager
        }

newtype AppT msg k v e a = AppT
    { unAppT :: StateT RaftState (ReaderT (AppConfig msg k v e) IO) a }
    deriving ( Functor, Applicative, Monad, MonadIO
             , MonadState RaftState
             , MonadReader (AppConfig msg k v e)
             )

-- TODO(DarinM223): change to actual snapshot type.
type SnapshotType = Int

instance HasSnapshotType SnapshotType (AppT msg k v e) where
    snapshotType = pure Proxy

$(deriveVia [t| forall msg k v e. ServerM msg ServerEvent (AppT msg k v e)
                            `Via` ServerT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (KeyClass k, ValueClass v) =>
                StorageM k v (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v. (KeyClass k, ValueClass v) =>
                ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v))
          `Via` StoreT (AppT msg k v (LogEntry k v)) |])
$(deriveVia [t| forall msg k v e. (Entry e) => LogM e (AppT msg k v e)
                                         `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. TempLogM e (AppT msg k v e)
                            `Via` TempLogT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. SnapshotM SnapshotType (AppT msg k v e)
                            `Via` SnapshotT (AppT msg k v e) |])

runAppT :: AppT msg k v e a
        -> AppConfig msg k v e
        -> RaftState
        -> IO (a, RaftState)
runAppT m config raftState = flip runReaderT config
                           . flip runStateT raftState
                           . unAppT
                           $ m

runAppTConfig :: AppT msg k v e a -> AppConfig msg k v e -> IO a
runAppTConfig m config = flip runReaderT config
                       . fmap fst
                       . flip runStateT emptyState
                       . unAppT
                       $ m
  where
    emptyState = newRaftState 0
