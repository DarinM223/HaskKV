{-# LANGUAGE FunctionalDependencies #-}
module HaskKV.Constr where

import Control.Monad.State (MonadIO, MonadState)
import Data.Map (Map)
import HaskKV.Log.Class (Entry, LogM, TempLogM)
import HaskKV.Raft.Class (PersistM, DebugM)
import HaskKV.Raft.State (RaftState)
import HaskKV.Server.Types (ServerEvent, ServerM)
import HaskKV.Snapshot.Types (SnapshotM)
import HaskKV.Store.Types

import qualified Data.Map as M

type Constr k v e = (KeyClass k, ValueClass v, Entry e)

class HasRun msg k v e c | c -> msg k v e where
  run :: c -> Fn msg k v e

type SnapshotType k v = Map k v

type FnConstr msg k v e m =
  ( MonadIO m
  , MonadState RaftState m
  , ServerM msg ServerEvent m
  , StorageM k v m
  , ApplyEntryM k v e m
  , LogM e m
  , LoadSnapshotM (SnapshotType k v) m
  , TakeSnapshotM m
  , TempLogM e m
  , SnapshotM (M.Map k v) m
  , DebugM m
  , PersistM m
  )
type Fn msg k v e = forall a. (forall m. (FnConstr msg k v e m) => m a) -> IO a
