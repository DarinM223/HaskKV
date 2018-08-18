module HaskKV.Store.Utils where

import Control.Concurrent.STM
import Data.Binary
import Data.Foldable (foldl')
import Data.Maybe (fromJust, fromMaybe)
import Data.Time
import HaskKV.Log.InMem
import HaskKV.Store.Types
import HaskKV.Types
import HaskKV.Utils

import qualified Data.Map as M
import qualified Data.Heap as H

-- | Updates the store with the given function and writes the log to disk.
persistAfter
  :: (Binary e)
  => (StoreData k v e -> StoreData k v e)
  -> Store k v e
  -> IO FileSize
persistAfter f (Store store) = do
  (sid, log) <- atomically $ do
    modifyTVar store f
    s <- readTVar store
    return (_sid s, _log s)
  persistBinary logFilename sid log

getKey :: (Ord k) => k -> StoreData k v e -> Maybe v
getKey k s = _map s M.!? k

setKey :: (Ord k, Storable v) => k -> v -> StoreData k v e -> StoreData k v e
setKey k v s = s { _map = M.insert k v' $ _map s, _heap = heap' }
 where
  maybeV    = _map s M.!? k
  maybeCas' = (+ 1) . version <$> maybeV
  v'        = fromMaybe v (setVersion <$> maybeCas' <*> pure v)
  heap'     = case expireTime v' of
    Just time -> H.insert (HeapVal (time, k)) $ _heap s
    _         -> _heap s

replaceKey
  :: (Ord k, Storable v)
  => k
  -> v
  -> StoreData k v e
  -> (Maybe CAS, StoreData k v e)
replaceKey k v s
  | equalCAS  = (Just cas', s { _map = M.insert k v' $ _map s })
  | otherwise = (Nothing, s)
 where
  maybeV   = _map s M.!? k
  equalCAS = fromMaybe True ((== version v) . version <$> maybeV)
  cas'     = version v + 1
  v'       = setVersion cas' v

deleteKey :: (Ord k) => k -> StoreData k v e -> StoreData k v e
deleteKey k s = s { _map = M.delete k $ _map s }

cleanupStore
  :: (Show k, Ord k, Storable v) => Time -> StoreData k v e -> StoreData k v e
cleanupStore curr s = case minHeapMaybe (_heap s) of
  Just (HeapVal (t, k)) | diff t curr <= 0 ->
    let
      (_, h') = fromJust . H.viewMin $ _heap s
      v       = getKey k s
    in 
        -- Only delete key from store if it hasn't been
        -- replaced/removed after it was set.
       if maybe False ((== 0) . diff t) (v >>= expireTime)
      then cleanupStore curr . deleteKey k $ s { _heap = h' }
      else cleanupStore curr s { _heap = h' }
  _ -> s
  where diff a b = realToFrac (diffUTCTime a b)

loadSnapshotStore
  :: (Ord k, Storable v)
  => LogIndex
  -> LogTerm
  -> M.Map k v
  -> StoreData k v e
  -> StoreData k v e
loadSnapshotStore lastIncludedIndex lastIncludedTerm map store =
  modifyLog (truncateLog lastIncludedIndex)
    . modifyLog
        (\l -> l
          { _snapshotLastIndex = Just lastIncludedIndex
          , _snapshotLastTerm  = Just lastIncludedTerm
          }
        )
    . foldl' (\store (k, v) -> setKey k v store) clearedStore
    . M.assocs
    $ map
 where
  clearedStore = store { _map = M.empty, _heap = H.empty }
  truncateLog lastIncludedIndex log
    | lastIncludedIndex > _lowIdx log = deleteRangeLog
      (_lowIdx log)
      lastIncludedIndex
      log
    | otherwise = log

modifyLog :: (Log e -> Log e) -> StoreData k v e -> StoreData k v e
modifyLog f store = store { _log = f (_log store) }
