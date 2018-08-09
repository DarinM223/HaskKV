module HaskKV.Constr where

import Control.Monad.IO.Class
import Data.Binary
import Data.Time
import HaskKV.Log
import HaskKV.Server
import HaskKV.Snapshot

import qualified Data.Map as M

type Time = UTCTime
type CAS  = Int
class Storable v where
    expireTime :: v -> Maybe Time
    version    :: v -> CAS
    setVersion :: CAS -> v -> v

type KeyClass k = (Show k, Ord k, Binary k)
type ValueClass v = (Show v, Storable v, Binary v)
type Constr k v e m = (KeyClass k, ValueClass v, Entry e, MonadIO m)

class TakeSnapshotM m where
    takeSnapshot :: m ()

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
type SnapFn k v = forall a. (forall m. (MonadIO m, SnapshotM (M.Map k v) m) => m a) -> IO a
