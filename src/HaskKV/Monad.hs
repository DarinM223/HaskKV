{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Monad where

import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Binary.Orphans ()
import Data.Deriving.Via
import Data.Map (Map)
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.Debug
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store
import HaskKV.Types

data AppConfig msg k v e = AppConfig
    { _store       :: Store k v e
    , _tempLog     :: TempLog e
    , _serverState :: ServerState msg
    , _isLeader    :: TVar Bool
    , _snapManager :: SnapshotManager
    }

instance HasServerState msg (AppConfig msg k v e) where
    getServerState = _serverState
instance HasStore k v e (AppConfig msg k v e) where
    getStore = _store
instance HasTempLog e (AppConfig msg k v e) where
    getTempLog = _tempLog
instance HasSnapshotManager (AppConfig msg k v e) where
    getSnapshotManager = _snapManager

newAppConfig :: Maybe FilePath
             -> Maybe (Log e)
             -> ServerState msg
             -> IO (AppConfig msg k v e)
newAppConfig snapshotDirectory initLog serverState = do
    isLeader <- newTVarIO False
    store <- newStore (getField @"_sid" serverState) initLog
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

type SnapshotType k v = Map k v
instance (Binary k, Binary v) =>
    HasSnapshotType (SnapshotType k v) (AppT msg k v e)

$(deriveVia [t| forall msg k v e. ServerM msg ServerEvent (AppT msg k v e)
                            `Via` ServerT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (KeyClass k, ValueClass v, Entry e) =>
                StorageM k v (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v. (KeyClass k, ValueClass v) =>
                ApplyEntryM k v (LogEntry k v) (AppT msg k v (LogEntry k v))
          `Via` StoreT (AppT msg k v (LogEntry k v)) |])
$(deriveVia [t| forall msg k v e. (Entry e, KeyClass k, ValueClass v) =>
                LogM e (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (KeyClass k, ValueClass v, Entry e) =>
                LoadSnapshotM (SnapshotType k v) (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (Entry e, KeyClass k, ValueClass v) =>
                TakeSnapshotM (AppT msg k v e)
          `Via` StoreT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. TempLogM e (AppT msg k v e)
                            `Via` TempLogT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. (Binary k, Binary v) =>
                SnapshotM (SnapshotType k v) (AppT msg k v e)
          `Via` SnapshotT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. DebugM (AppT msg k v e)
                            `Via` PrintDebugT (AppT msg k v e) |])
$(deriveVia [t| forall msg k v e. PersistM (AppT msg k v e)
                            `Via` PersistT (AppT msg k v e) |])

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
    emptyState = newRaftState (SID 0) Nothing
