module HaskKV.Constr where

import Control.Monad.IO.Class
import HaskKV.Log
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

import qualified Data.Map as M

type Constr k v e m = (KeyClass k, ValueClass v, Entry e, MonadIO m)

class HasRun msg k v e c | c -> msg k v e where
    getRun :: c -> Fn msg k v e

type FnConstr msg k v e m =
    ( MonadIO m
    , SnapshotM (M.Map k v) m
    , TakeSnapshotM m
    , ServerM msg ServerEvent m
    , LogM e m
    , TempLogM e m
    )
type Fn msg k v e = forall a. (forall m. (FnConstr msg k v e m) => m a) -> IO a
type SnapFn k v =
    forall a. (forall m. (MonadIO m, SnapshotM (M.Map k v) m) => m a) -> IO a
