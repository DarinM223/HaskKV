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

type StoreClass k v e cfg m =
  ( MonadIO m
  , HasStore k v e cfg
  , MonadState RaftState m
  , KeyClass k
  , ValueClass v
  , Entry e
  )

mkStorageM :: StoreClass k v e cfg m => cfg -> StorageM k v m
mkStorageM cfg = StorageM
  { getValue       = getValue' cfg
  , setValue       = setValue' cfg
  , replaceValue   = replaceValue' cfg
  , deleteValue    = deleteValue' cfg
  , cleanupExpired = cleanupExpired' cfg
  }

mkLogM :: StoreClass k v e cfg m
       => SnapshotM s m -> TakeSnapshotM m -> cfg -> LogM e m
mkLogM snapM takeSnapM cfg = LogM
  { firstIndex    = firstIndex' cfg
  , lastIndex     = lastIndex' cfg
  , loadEntry     = loadEntry' cfg
  , termFromIndex = termFromIndex' cfg
  , deleteRange   = deleteRange' cfg
  , storeEntries  = storeEntries' snapM takeSnapM cfg
  }

type SClass k v e cfg m = (HasStore k v e cfg, MonadIO m)

getValue' :: (SClass k v e cfg m, Ord k) => cfg -> k -> m (Maybe v)
getValue' cfg k =
  fmap (getKey k) . liftIO . readTVarIO . unStore . getStore $ cfg

setValue' :: (SClass k v e cfg m, Ord k, Storable v) => cfg -> k -> v -> m ()
setValue' cfg k v =
  liftIO . modifyTVarIO (setKey k v) . unStore . getStore $ cfg

replaceValue' :: (SClass k v e cfg m, Ord k, Storable v)
              => cfg -> k -> v -> m (Maybe CAS)
replaceValue' cfg k v =
  liftIO . stateTVarIO (replaceKey k v) . unStore . getStore $ cfg

deleteValue' :: (SClass k v e cfg m, Ord k) => cfg -> k -> m ()
deleteValue' cfg k =
  liftIO . modifyTVarIO (deleteKey k) . unStore . getStore $ cfg

cleanupExpired' :: (SClass k v e cfg m, Show k, Ord k, Storable v)
                => cfg -> Time -> m ()
cleanupExpired' cfg t =
  liftIO . modifyTVarIO (cleanupStore t) . unStore . getStore $ cfg

firstIndex' :: SClass k v e cfg m => cfg -> m LogIndex
firstIndex' cfg =
  fmap (_lowIdx . _log) . liftIO . readTVarIO . unStore . getStore $ cfg

lastIndex' :: SClass k v e cfg m => cfg -> m LogIndex
lastIndex' cfg =
  fmap (lastIndexLog . _log) . liftIO . readTVarIO . unStore . getStore $ cfg

loadEntry' :: SClass k v e cfg m => cfg -> LogIndex -> m (Maybe e)
loadEntry' cfg (LogIndex k) = fmap (IM.lookup k . _entries . _log)
                            . liftIO . readTVarIO . unStore . getStore $ cfg

termFromIndex' :: (SClass k v e cfg m, Entry e)
               => cfg -> LogIndex -> m (Maybe LogTerm)
termFromIndex' cfg i =
  fmap (entryTermLog i . _log) . liftIO . readTVarIO . unStore . getStore $ cfg

storeEntries' :: (SClass k v e cfg m, Entry e)
              => SnapshotM s m
              -> TakeSnapshotM m
              -> cfg
              -> [e]
              -> m ()
storeEntries' snapM (TakeSnapshotM takeSnapshot) cfg es = do
  let store = getStore cfg
  logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
  snapshotInfo snapM >>= \case
    Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
    _ -> return ()

deleteRange' :: (SClass k v e cfg m, Binary e)
             => cfg -> LogIndex -> LogIndex -> m ()
deleteRange' cfg a b =
  liftIO $ void $ persistAfter (modifyLog (deleteRangeLog a b)) $ getStore cfg

applyEntry :: MonadIO m => StorageM k v m -> LogEntry k v -> m ()
applyEntry storeM LogEntry { _data = entry, _completed = Completed lock } = do
  mapM_ (liftIO . atomically . flip putTMVar ()) lock
  applyStore entry
 where
  applyStore (Change _ k v) = setValue storeM k v
  applyStore (Delete _ k  ) = deleteValue storeM k
  applyStore _              = return ()

loadSnapshot :: (SClass k v e cfg m, Ord k, Storable v)
             => cfg
             -> LogIndex
             -> LogTerm
             -> M.Map k v
             -> m ()
loadSnapshot cfg lastIndex lastTerm map
  = liftIO . atomically
  . flip modifyTVar' (loadSnapshotStore lastIndex lastTerm map)
  . unStore
  $ getStore cfg

takeSnapshot :: ( SClass k v e cfg m
                , KeyClass k, ValueClass v, Entry e
                , MonadState RaftState m )
             => SnapshotM (M.Map k v) m
             -> (forall a. m a -> IO a)
             -> cfg
             -> m ()
takeSnapshot snapM run cfg = do
  let store = getStore cfg
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
