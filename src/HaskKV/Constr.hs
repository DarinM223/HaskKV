module HaskKV.Constr where

import Control.Monad.IO.Class
import Data.Map (Map)
import HaskKV.Log.Class
import HaskKV.Raft.Class
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

import qualified Data.Map as M

type Constr k v e m = (KeyClass k, ValueClass v, Entry e, MonadIO m)

class HasRun msg k v e c | c -> msg k v e where
  getRun :: c -> Fn msg k v e

type SnapshotType k v = Map k v

type FnConstr msg k v e m =
  ( MonadIO m
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
