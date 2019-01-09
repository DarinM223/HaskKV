module HaskKV.Store.Instances where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary
import Data.Maybe (fromJust)
import HaskKV.Constr
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

type StoreClass k v e r m =
  ( MonadIO m
  , MonadReader r m
  , HasStore k v e r
  , MonadState RaftState m
  , KeyClass k
  , ValueClass v
  , Entry e
  )

storageMIO :: StoreClass k v e r m => StorageM k v m
storageMIO = StorageM
  { getValue       = getValue'
  , setValue       = setValue'
  , replaceValue   = replaceValue'
  , deleteValue    = deleteValue'
  , cleanupExpired = cleanupExpired'
  }

logMIO :: StoreClass k v e r m => SnapshotM s m -> TakeSnapshotM m -> LogM e m
logMIO snapM takeSnapM = LogM
  { firstIndex    = firstIndex'
  , lastIndex     = lastIndex'
  , loadEntry     = loadEntry'
  , termFromIndex = termFromIndex'
  , deleteRange   = deleteRange'
  , storeEntries  = storeEntries' snapM takeSnapM
  }

getValue' :: (Ord k, HasStore k v e r, MonadReader r m, MonadIO m)
          => k -> m (Maybe v)
getValue' k = asks getStore >>= fmap (getKey k) . liftIO . readTVarIO . unStore

setValue' :: (Ord k, Storable v, HasStore k v e r, MonadReader r m, MonadIO m)
          => k -> v -> m ()
setValue' k v = asks getStore >>= liftIO . modifyTVarIO (setKey k v) . unStore

replaceValue'
  :: (Ord k, Storable v, HasStore k v e r, MonadReader r m, MonadIO m)
  => k -> v -> m (Maybe CAS)
replaceValue' k v = asks getStore >>=
  liftIO . stateTVarIO (replaceKey k v) . unStore

deleteValue' :: (Ord k, HasStore k v e r, MonadReader r m, MonadIO m)
             => k -> m ()
deleteValue' k = asks getStore >>= liftIO . modifyTVarIO (deleteKey k) . unStore

cleanupExpired'
  :: (Show k, Ord k, Storable v, HasStore k v e r, MonadReader r m, MonadIO m)
  => Time -> m ()
cleanupExpired' t = asks getStore >>=
  liftIO . modifyTVarIO (cleanupStore t) . unStore

firstIndex' :: (HasStore k v e r, MonadReader r m, MonadIO m) => m LogIndex
firstIndex' = asks getStore >>=
  fmap (_lowIdx . _log) . liftIO . readTVarIO . unStore

lastIndex' :: (HasStore k v e r, MonadReader r m, MonadIO m) => m LogIndex
lastIndex' = asks getStore >>=
  fmap (lastIndexLog . _log) . liftIO . readTVarIO . unStore

loadEntry' :: (HasStore k v e r, MonadReader r m, MonadIO m)
           => LogIndex -> m (Maybe e)
loadEntry' (LogIndex k) = asks getStore >>=
  fmap (IM.lookup k . _entries . _log) . liftIO . readTVarIO . unStore

termFromIndex' :: (Entry e, HasStore k v e r, MonadReader r m, MonadIO m)
               => LogIndex -> m (Maybe LogTerm)
termFromIndex' i = asks getStore >>=
  fmap (entryTermLog i . _log) . liftIO . readTVarIO . unStore

storeEntries'
  :: (Entry e, MonadIO m, HasStore k v e r, MonadReader r m)
  => SnapshotM s m
  -> TakeSnapshotM m
  -> [e]
  -> m ()
storeEntries' snapM (TakeSnapshotM takeSnapshot) es = do
  store <- asks getStore
  logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
  snapshotInfo snapM >>= \case
    Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
    _ -> return ()

deleteRange' :: (Binary e, HasStore k v e r, MonadReader r m, MonadIO m)
             => LogIndex -> LogIndex -> m ()
deleteRange' a b = asks getStore >>=
  void . liftIO . persistAfter (modifyLog (deleteRangeLog a b))

applyEntry :: MonadIO m => StorageM k v m -> LogEntry k v -> m ()
applyEntry storeM LogEntry { _data = entry, _completed = Completed lock } = do
  mapM_ (liftIO . atomically . flip putTMVar ()) lock
  applyStore entry
 where
  applyStore (Change _ k v) = setValue storeM k v
  applyStore (Delete _ k  ) = deleteValue storeM k
  applyStore _              = return ()

loadSnapshot
  :: (Ord k, Storable v, HasStore k v e r, MonadReader r m, MonadIO m)
  => LogIndex
  -> LogTerm
  -> M.Map k v
  -> m ()
loadSnapshot lastIndex lastTerm map
  =   asks getStore
  >>= liftIO . atomically
  .   flip modifyTVar' (loadSnapshotStore lastIndex lastTerm map)
  .   unStore

class HasRun m r where getRun :: r -> (forall a. m a -> IO a)

takeSnapshot
  :: ( KeyClass k, ValueClass v, Entry e
     , HasStore k v e r, HasRun m r
     , MonadState RaftState m, MonadReader r m, MonadIO m )
  => SnapshotM (M.Map k v) m -> m ()
takeSnapshot snapM = do
  store <- asks getStore
  run <- asks getRun
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
