{-# LANGUAGE AllowAmbiguousTypes #-}

module HaskKV.Store.Instances where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary
import Data.Generics.Product.Typed
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
  , HasType (Store k v e) r
  , MonadState RaftState m
  , KeyClass k
  , ValueClass v
  , Entry e
  )

storageMIO :: forall k v e r m. StoreClass k v e r m => StorageM k v m
storageMIO = StorageM
  { getValue       = getValue' @_ @_ @e
  , setValue       = setValue' @_ @_ @e
  , replaceValue   = replaceValue' @_ @_ @e
  , deleteValue    = deleteValue' @_ @v @e
  , cleanupExpired = cleanupExpired' @k @v @e
  }

logMIO :: forall k v e r s m. StoreClass k v e r m
       => SnapshotM s m -> TakeSnapshotM m -> LogM e m
logMIO snapM takeSnapM = LogM
  { firstIndex    = firstIndex' @k @v @e
  , lastIndex     = lastIndex' @k @v @e
  , loadEntry     = loadEntry' @k @v
  , termFromIndex = termFromIndex' @k @v @e
  , deleteRange   = deleteRange' @k @v @e
  , storeEntries  = storeEntries' @k @v snapM takeSnapM
  }

type SClass k v e r m = (HasType (Store k v e) r, MonadReader r m, MonadIO m)

getS :: forall k v e r m a. SClass k v e r m => (Store k v e -> IO a) -> m a
getS m = asks (getTyped @(Store k v e)) >>= liftIO . m

getValue' :: forall k v e r m. (SClass k v e r m, Ord k) => k -> m (Maybe v)
getValue' k = getS @_ @_ @e $ fmap (getKey k) . readTVarIO . unStore

setValue' :: forall k v e r m. (SClass k v e r m, Ord k, Storable v)
          => k -> v -> m ()
setValue' k v = getS @_ @_ @e $ modifyTVarIO (setKey k v) . unStore

replaceValue'
  :: forall k v e r m. (SClass k v e r m, Ord k, Storable v)
  => k -> v -> m (Maybe CAS)
replaceValue' k v = getS @_ @_ @e $ stateTVarIO (replaceKey k v) . unStore

deleteValue' :: forall k v e r m. (SClass k v e r m, Ord k)
             => k -> m ()
deleteValue' k = getS @_ @v @e $ modifyTVarIO (deleteKey k) . unStore

cleanupExpired'
  :: forall k v e r m. (SClass k v e r m, Show k, Ord k, Storable v)
  => Time -> m ()
cleanupExpired' t = getS @k @v @e $ modifyTVarIO (cleanupStore t) . unStore

firstIndex' :: forall k v e r m. SClass k v e r m => m LogIndex
firstIndex' = getS @k @v @e $ fmap (_lowIdx . _log) . readTVarIO . unStore

lastIndex' :: forall k v e r m. SClass k v e r m => m LogIndex
lastIndex' = getS @k @v @e $ fmap (lastIndexLog . _log) . readTVarIO . unStore

loadEntry' :: forall k v e r m. SClass k v e r m => LogIndex -> m (Maybe e)
loadEntry' (LogIndex k) = getS @k @v $
  fmap (IM.lookup k . _entries . _log) . readTVarIO . unStore

termFromIndex' :: forall k v e r m. (SClass k v e r m, Entry e)
               => LogIndex -> m (Maybe LogTerm)
termFromIndex' i = getS @k @v @e $
  fmap (entryTermLog i . _log) . readTVarIO . unStore

storeEntries'
  :: forall k v e r s m. (SClass k v e r m, Entry e)
  => SnapshotM s m
  -> TakeSnapshotM m
  -> [e]
  -> m ()
storeEntries' snapM (TakeSnapshotM takeSnapshot) es = do
  store <- asks (getTyped @(Store k v e))
  logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
  snapshotInfo snapM >>= \case
    Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
    _ -> return ()

deleteRange' :: forall k v e r m. (SClass k v e r m, Binary e)
             => LogIndex -> LogIndex -> m ()
deleteRange' a b = getS @k @v @e $
  void . persistAfter (modifyLog (deleteRangeLog a b))

applyEntry :: MonadIO m => StorageM k v m -> LogEntry k v -> m ()
applyEntry storeM LogEntry { _data = entry, _completed = Completed lock } = do
  mapM_ (liftIO . atomically . flip putTMVar ()) lock
  applyStore entry
 where
  applyStore (Change _ k v) = setValue storeM k v
  applyStore (Delete _ k  ) = deleteValue storeM k
  applyStore _              = return ()

loadSnapshot
  :: forall k v e r m. (SClass k v e r m, Ord k, Storable v)
  => LogIndex
  -> LogTerm
  -> M.Map k v
  -> m ()
loadSnapshot lastIndex lastTerm map
  = getS @_ @_ @e
  $ liftIO . atomically
  . flip modifyTVar' (loadSnapshotStore lastIndex lastTerm map)
  . unStore

takeSnapshot
  :: forall k v e r m
   . ( SClass k v e r m
     , KeyClass k, ValueClass v, Entry e
     , HasRun m r, MonadState RaftState m )
  => SnapshotM (M.Map k v) m -> m ()
takeSnapshot snapM = do
  store <- asks (getTyped @(Store k v e))
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
