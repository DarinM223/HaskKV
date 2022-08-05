{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Store.Instances where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM (atomically, readTVarIO, putTMVar, modifyTVar')
import Control.Monad.IO.Unlift (MonadUnliftIO, withRunInIO)
import Control.Monad.Reader
import Control.Monad.State (MonadState, gets)
import Data.Binary (Binary, encode)
import Data.Foldable (for_, traverse_)
import Data.Maybe (fromJust)
import HaskKV.Log.Class (Entry (entryTerm), LogM (..))
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.State (RaftState (..))
import HaskKV.Snapshot.Types
import HaskKV.Store.Types
import HaskKV.Store.Utils
import HaskKV.Types (LogIndex (..), LogTerm)
import HaskKV.Utils (modifyTVarIO, stateTVarIO)
import Optics ((%), (^.), At (at), ViewableOptic (gview))

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
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
type SnapFn k v =
  forall a. (forall m. (MonadIO m, SnapshotM (M.Map k v) m) => m a) -> IO a

instance (StoreClass k v e r m) => StorageM k v (StoreT m) where
  getValue k = gview storeL >>= liftIO . getValue' k
  setValue k v = gview storeL >>= liftIO . setValue' k v
  replaceValue k v = gview storeL >>= liftIO . replaceValue' k v
  deleteValue k = gview storeL >>= liftIO . deleteValue' k
  cleanupExpired t = gview storeL >>= liftIO . cleanupExpired' t

instance (StoreClass k v e r m, SnapshotM s m, TakeSnapshotM m)
  => LogM e (StoreT m) where
  firstIndex = gview storeL >>= liftIO . firstIndex'
  lastIndex = gview storeL >>= liftIO . lastIndex'
  loadEntry k = gview storeL >>= liftIO . loadEntry' k
  termFromIndex i = gview storeL >>= liftIO . termFromIndex' i
  deleteRange a b = gview storeL >>= liftIO . deleteRange' a b
  storeEntries es = gview storeL >>= lift . storeEntries' es

instance ( StoreClass k v e r m
         , SnapshotM s m
         , TakeSnapshotM m
         , e ~ LogEntry k v
         ) => ApplyEntryM k v e (StoreT m) where
  applyEntry = applyEntry'

instance (StoreClass k v e r m) => LoadSnapshotM (M.Map k v) (StoreT m) where
  loadSnapshot i t map = gview storeL >>= liftIO . loadSnapshot' i t map
  {-# INLINABLE loadSnapshot #-}

instance (StoreClass k v e r m, MonadUnliftIO m, SnapshotM (M.Map k v) m)
  => TakeSnapshotM (StoreT m) where
  takeSnapshot = lift $ do
    store <- gview storeL
    lastApplied <- gets (^. #lastApplied)
    takeSnapshot' lastApplied store

getValue' :: (Ord k) => k -> Store k v e -> IO (Maybe v)
getValue' k = fmap (getKey k) . readTVarIO . unStore

setValue' :: (Ord k, Storable v) => k -> v -> Store k v e -> IO ()
setValue' k v = modifyTVarIO (setKey k v) . unStore

replaceValue' :: (Ord k, Storable v) => k -> v -> Store k v e -> IO (Maybe CAS)
replaceValue' k v = stateTVarIO (replaceKey k v) . unStore

deleteValue' :: (Ord k) => k -> Store k v e -> IO ()
deleteValue' k = modifyTVarIO (deleteKey k) . unStore

cleanupExpired' :: (Show k, Ord k, Storable v) => Time -> Store k v e -> IO ()
cleanupExpired' t = modifyTVarIO (cleanupStore t) . unStore

firstIndex' :: Store k v e -> IO LogIndex
firstIndex' = fmap (^. #log % #lowIdx) . readTVarIO . unStore

lastIndex' :: Store k v e -> IO LogIndex
lastIndex' = fmap (lastIndexLog . (^. #log)) . readTVarIO . unStore

loadEntry' :: LogIndex -> Store k v e -> IO (Maybe e)
loadEntry' (LogIndex k) =
  fmap (^. #log % #entries % at k) . readTVarIO . unStore

termFromIndex' :: (Entry e) => LogIndex -> Store k v e -> IO (Maybe LogTerm)
termFromIndex' i = fmap (entryTermLog i . (^. #log)) . readTVarIO . unStore

storeEntries'
  :: (Entry e, MonadIO m, SnapshotM s m, TakeSnapshotM m)
  => [e] -> Store k v e -> m ()
storeEntries' es store = do
  logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
  snapshotInfo >>= \case
    Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
    _ -> return ()

deleteRange' :: (Binary e) => LogIndex -> LogIndex -> Store k v e -> IO ()
deleteRange' a b = void . persistAfter (modifyLog (deleteRangeLog a b))

applyEntry' :: (MonadIO m, StorageM k v m) => LogEntry k v -> m ()
applyEntry' LogEntry { entryData = entry
                     , completed = Completed lock } = do
  traverse_ (liftIO . atomically . flip putTMVar ()) lock
  applyStore entry
 where
  applyStore (Change _ k v) = setValue k v
  applyStore (Delete _ k  ) = deleteValue k
  applyStore _              = return ()

loadSnapshot'
  :: (Ord k, Storable v)
  => LogIndex -> LogTerm -> M.Map k v -> Store k v e -> IO ()
loadSnapshot' lastIndex lastTerm map (Store store) =
  atomically $ modifyTVar' store (loadSnapshotStore lastIndex lastTerm map)

takeSnapshot'
  :: ( KeyClass k, ValueClass v, Entry e
     , MonadUnliftIO m
     , SnapshotM (M.Map k v) m
     )
  => LogIndex -> Store k v e -> m ()
takeSnapshot' lastApplied store = do
  storeData <- liftIO $ readTVarIO $ unStore store
  let firstIndex = storeData ^. #log % #lowIdx
      lastTerm   = entryTerm $ fromJust
                 $ storeData ^. #log % #entries % at (unLogIndex lastApplied)
  withRunInIO $ \run ->
    void $ forkIO $ run $ do
      createSnapshot lastApplied lastTerm
      let snapData = B.concat . BL.toChunks . encode $ storeData ^. #map
      -- FIXME(DarinM223): maybe write a version of writeSnapshot
      -- that takes in a lazy bytestring instead of a strict bytestring?
      writeSnapshot 0 snapData lastApplied
      saveSnapshot lastApplied

      snap <- readSnapshot lastApplied
      for_ snap $ \snap ->
        liftIO
          $ flip persistAfter store
          $ loadSnapshotStore lastApplied lastTerm snap
          . modifyLog (deleteRangeLog firstIndex lastApplied)
