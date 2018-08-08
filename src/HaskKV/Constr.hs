module HaskKV.Constr where

class TakeSnapshotM m where
    takeSnapshot :: m ()

class HasRun c where
    getRun :: c -> RunFn

newtype RunFn = RunFn (forall a. (forall m. (TakeSnapshotM m) => m a) -> IO a)
