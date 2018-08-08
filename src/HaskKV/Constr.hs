module HaskKV.Constr where

import Control.Monad.IO.Class
import Data.Binary
import Data.Time
import HaskKV.Log
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

class HasRun k v e c | c -> k v e where
    getRun :: c -> RunFn k v e

newtype RunFn k v e = RunFn (forall a. (forall m. (MonadIO m, SnapshotM (M.Map k v) m, TakeSnapshotM m) => m a) -> IO a)
