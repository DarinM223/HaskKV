{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Store.Utils where

import Control.Concurrent.STM (atomically, readTVar, modifyTVar)
import Data.Binary (Binary)
import Data.Foldable (foldl')
import Data.Maybe (fromJust, fromMaybe)
import Data.Time (diffUTCTime)
import HaskKV.Log.InMem
import HaskKV.Store.Types
import HaskKV.Types (FileSize, LogIndex, LogTerm)
import HaskKV.Utils (minHeapMaybe, persistBinary)
import Optics ((&), (%~), (.~), (^.))

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
    return (s ^. #sid, s ^. #log)
  persistBinary logFilename sid log

getKey :: (Ord k) => k -> StoreData k v e -> Maybe v
getKey k s = (s ^. #map) M.!? k

setKey :: (Ord k, Storable v) => k -> v -> StoreData k v e -> StoreData k v e
setKey k v s = s & #map %~ M.insert k v' & #heap .~ heap'
 where
  maybeV    = (s ^. #map) M.!? k
  maybeCas' = (+ 1) . version <$> maybeV
  v'        = fromMaybe v (setVersion <$> maybeCas' <*> pure v)
  heap'     = case expireTime v' of
    Just time -> H.insert (HeapVal (time, k)) $ s ^. #heap
    _         -> s ^. #heap

replaceKey
  :: (Ord k, Storable v)
  => k
  -> v
  -> StoreData k v e
  -> (Maybe CAS, StoreData k v e)
replaceKey k v s
  | equalCAS  = (Just cas', s & #map %~ M.insert k v')
  | otherwise = (Nothing, s)
 where
  maybeV   = (s ^. #map) M.!? k
  equalCAS = fromMaybe True ((== version v) . version <$> maybeV)
  cas'     = version v + 1
  v'       = setVersion cas' v

deleteKey :: (Ord k) => k -> StoreData k v e -> StoreData k v e
deleteKey k = #map %~ M.delete k

cleanupStore
  :: (Show k, Ord k, Storable v) => Time -> StoreData k v e -> StoreData k v e
cleanupStore curr s = case minHeapMaybe (s ^. #heap) of
  Just (HeapVal (t, k)) | diff t curr <= 0 ->
    let (_, h') = fromJust . H.viewMin $ s ^. #heap
        v       = getKey k s
    in
       -- Only delete key from store if it hasn't been
       -- replaced/removed after it was set.
       if maybe False ((== 0) . diff t) (v >>= expireTime)
      then cleanupStore curr . deleteKey k $ s & #heap .~ h'
      else cleanupStore curr $ s & #heap .~ h'
  _ -> s
  where diff a b = realToFrac (diffUTCTime a b)

loadSnapshotStore
  :: (Ord k, Storable v)
  => LogIndex
  -> LogTerm
  -> M.Map k v
  -> StoreData k v e
  -> StoreData k v e
loadSnapshotStore lastIncludedIndex lastIncludedTerm map store
  = modifyLog (truncateLog lastIncludedIndex)
  . modifyLog
      (\l -> l
        { snapshotLastIndex = Just lastIncludedIndex
        , snapshotLastTerm  = Just lastIncludedTerm
        }
      )
  . foldl' (\store (k, v) -> setKey k v store) clearedStore
  $ M.assocs map
 where
  clearedStore = store & #map .~ M.empty & #heap .~ H.empty
  truncateLog lastIncludedIndex log
    | lastIncludedIndex > log ^. #lowIdx = deleteRangeLog
      (log ^. #lowIdx)
      lastIncludedIndex
      log
    | otherwise = log

modifyLog :: (Log e -> Log e) -> StoreData k v e -> StoreData k v e
modifyLog f = #log %~ f
