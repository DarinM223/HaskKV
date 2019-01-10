module HaskKV.Constr where

import Control.Monad.State
import Data.Map (Map)
import HaskKV.Log.Class
import HaskKV.Raft.Class
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

import qualified Data.Map as M

type SnapshotType k v = Map k v

class HasRun m r where getRun :: r -> (forall a. m a -> IO a)
