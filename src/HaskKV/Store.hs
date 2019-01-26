module HaskKV.Store
  ( mkStore
  , mkStoreValue
  , mkLogM
  , mkStorageM
  , applyEntry'
  , loadSnapshot'
  , takeSnapshot'
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary
import Data.Foldable (foldl')
import Data.Maybe (fromJust, fromMaybe)
import Data.Time
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.State (RaftState(..))
import HaskKV.Snapshot.Types
import HaskKV.Store.Types
import HaskKV.Types
import HaskKV.Utils

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM
import qualified Data.Heap as H
import qualified Data.Map as M

mkStoreValue :: Integer -> Int -> v -> IO (StoreValue v)
mkStoreValue seconds version val = do
  currTime <- getCurrentTime
  let newTime = addUTCTime diff currTime
  return StoreValue
    { _version    = version
    , _expireTime = Just newTime
    , _value      = val
    }
  where diff = fromRational . toRational . secondsToDiffTime $ seconds

mkStore :: SID -> Maybe (Log e) -> IO (Store k v e)
mkStore sid log = fmap Store . newTVarIO $ mkStoreData sid log

mkStoreData :: SID -> Maybe (Log e) -> StoreData k v e
mkStoreData sid log = StoreData
  { _map         = M.empty
  , _heap        = H.empty
  , _log         = fromMaybe emptyLog log
  , _tempEntries = []
  , _sid         = sid
  }

type StoreClass k v e m =
  ( MonadIO m
  , MonadState RaftState m
  , KeyClass k
  , ValueClass v
  , Entry e
  )

mkStorageM :: StoreClass k v e m => Store k v e -> StorageM k v m
mkStorageM store = StorageM
  { getValue       = getValue' store
  , setValue       = setValue' store
  , replaceValue   = replaceValue' store
  , deleteValue    = deleteValue' store
  , cleanupExpired = cleanupExpired' store
  }

mkLogM :: StoreClass k v e m
       => SnapshotM s m -> TakeSnapshotM m -> Store k v e -> LogM e m
mkLogM snapM takeSnapM store = LogM
  { firstIndex    = firstIndex' store
  , lastIndex     = lastIndex' store
  , loadEntry     = loadEntry' store
  , termFromIndex = termFromIndex' store
  , deleteRange   = deleteRange' store
  , storeEntries  = storeEntries' snapM takeSnapM store
  }

getValue' :: (MonadIO m, Ord k) => Store k v e -> k -> m (Maybe v)
getValue' (Store store) k = fmap (getKey k) $ liftIO $ readTVarIO store

setValue' :: (MonadIO m, Ord k, Storable v) => Store k v e -> k -> v -> m ()
setValue' (Store store) k v = liftIO $ modifyTVarIO (setKey k v) store

replaceValue' :: (MonadIO m, Ord k, Storable v)
              => Store k v e -> k -> v -> m (Maybe CAS)
replaceValue' (Store store) k v = liftIO $ stateTVarIO (replaceKey k v) store

deleteValue' :: (MonadIO m, Ord k) => Store k v e -> k -> m ()
deleteValue' (Store store) k = liftIO $ modifyTVarIO (deleteKey k) store

cleanupExpired' :: (MonadIO m, Show k, Ord k, Storable v)
                => Store k v e -> Time -> m ()
cleanupExpired' (Store store) t = liftIO $ modifyTVarIO (cleanupStore t) store

firstIndex' :: MonadIO m => Store k v e -> m LogIndex
firstIndex' (Store store) = fmap (_lowIdx . _log) $ liftIO $ readTVarIO store

lastIndex' :: MonadIO m => Store k v e -> m LogIndex
lastIndex' (Store store) =
  fmap (lastIndexLog . _log) $ liftIO $ readTVarIO store

loadEntry' :: MonadIO m => Store k v e -> LogIndex -> m (Maybe e)
loadEntry' (Store store) (LogIndex k) = fmap (IM.lookup k . _entries . _log)
                                      $ liftIO $ readTVarIO store

termFromIndex' :: (MonadIO m, Entry e)
               => Store k v e -> LogIndex -> m (Maybe LogTerm)
termFromIndex' (Store store) i =
  fmap (entryTermLog i . _log) $ liftIO $ readTVarIO store

storeEntries' :: (MonadIO m, Entry e)
              => SnapshotM s m
              -> TakeSnapshotM m
              -> Store k v e
              -> [e]
              -> m ()
storeEntries' snapM (TakeSnapshotM takeSnapshot) store es = do
  logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
  snapshotInfo snapM >>= \case
    Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
    _ -> return ()

deleteRange' :: (MonadIO m, Binary e)
             => Store k v e -> LogIndex -> LogIndex -> m ()
deleteRange' store a b =
  liftIO $ void $ persistAfter (modifyLog (deleteRangeLog a b)) store

applyEntry' :: MonadIO m => StorageM k v m -> LogEntry k v -> m ()
applyEntry' storeM LogEntry { _data = entry, _completed = Completed lock } = do
  mapM_ (liftIO . atomically . flip putTMVar ()) lock
  applyStore entry
 where
  applyStore (Change _ k v) = setValue storeM k v
  applyStore (Delete _ k  ) = deleteValue storeM k
  applyStore _              = return ()

loadSnapshot' :: (MonadIO m, Ord k, Storable v)
              => Store k v e
              -> LogIndex
              -> LogTerm
              -> M.Map k v
              -> m ()
loadSnapshot' (Store store) lastIndex lastTerm map
  = liftIO
  . atomically
  . flip modifyTVar' (loadSnapshotStore lastIndex lastTerm map)
  $ store

takeSnapshot' :: ( MonadIO m, MonadState RaftState m
                 , KeyClass k, ValueClass v, Entry e )
              => SnapshotM (M.Map k v) m
              -> (forall a. m a -> IO a)
              -> Store k v e
              -> m ()
takeSnapshot' snapM run store = do
  lastApplied <- gets _lastApplied
  storeData <- liftIO $ readTVarIO $ unStore store
  let firstIndex = _lowIdx $ _log storeData
      lastTerm   = entryTerm
                 . fromJust . IM.lookup (unLogIndex lastApplied)
                 . _entries . _log
                 $ storeData
  void $ liftIO $ forkIO $ run $ do
    createSnapshot snapM lastApplied lastTerm
    let snapData = B.concat . BL.toChunks . encode . _map $ storeData
    -- FIXME(DarinM223): maybe write a version of writeSnapshot
    -- that takes in a lazy bytestring instead of a strict bytestring?
    writeSnapshot snapM 0 snapData lastApplied
    saveSnapshot snapM lastApplied

    snap <- readSnapshot snapM lastApplied
    forM_ snap $ \snap ->
      liftIO
        $ flip persistAfter store
        $ loadSnapshotStore lastApplied lastTerm snap
        . modifyLog (deleteRangeLog firstIndex lastApplied)

-- | Updates the store with the given function and writes the log to disk.
persistAfter :: Binary e
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

replaceKey :: (Ord k, Storable v)
           => k -> v -> StoreData k v e -> (Maybe CAS, StoreData k v e)
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
  => LogIndex -> LogTerm -> M.Map k v -> StoreData k v e -> StoreData k v e
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
