module HaskKV.Store.Instances where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary
import Data.Maybe (fromJust)
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.State (RaftState(..))
import HaskKV.Snapshot.Types
import HaskKV.Store.Types
import HaskKV.Store.Utils
import HaskKV.Types
import HaskKV.Utils

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM
import qualified Data.Map as M

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
