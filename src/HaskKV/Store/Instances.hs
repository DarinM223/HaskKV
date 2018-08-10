{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Store.Instances where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary
import Data.Maybe (fromJust)
import HaskKV.Constr
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.State hiding (Time)
import HaskKV.Snapshot.Types
import HaskKV.Store.Types
import HaskKV.Store.Utils
import HaskKV.Types
import HaskKV.Utils

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM
import qualified Data.Map as M

newtype StoreT m a = StoreT { unStoreT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance MonadTrans StoreT where
    lift = StoreT

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
    getValue k = liftIO . getValueImpl k =<< asks getStore
    setValue k v = liftIO . setValueImpl k v =<< asks getStore
    replaceValue k v = liftIO . replaceValueImpl k v =<< asks getStore
    deleteValue k = liftIO . deleteValueImpl k =<< asks getStore
    cleanupExpired t = liftIO . cleanupExpiredImpl t =<< asks getStore

instance (StoreClass k v e r m, SnapshotM s m, TakeSnapshotM m)
    => LogM e (StoreT m) where
    firstIndex = liftIO . firstIndexImpl =<< asks getStore
    lastIndex = liftIO . lastIndexImpl =<< asks getStore
    loadEntry k = liftIO . loadEntryImpl k =<< asks getStore
    termFromIndex i = liftIO . termFromIndexImpl i =<< asks getStore
    deleteRange a b = liftIO . deleteRangeImpl a b =<< asks getStore
    storeEntries es = lift . storeEntriesImpl es =<< asks getStore

instance ( StoreClass k v e r m
         , SnapshotM s m
         , TakeSnapshotM m
         , e ~ LogEntry k v
         ) => ApplyEntryM k v e (StoreT m) where
    applyEntry = applyEntryImpl

instance (StoreClass k v e r m) => LoadSnapshotM (M.Map k v) (StoreT m) where
    loadSnapshot i t map = liftIO . loadSnapshotImpl i t map =<< asks getStore

instance (StoreClass k v e r m, HasRun msg k v e r)
    => TakeSnapshotM (StoreT m) where
    takeSnapshot = do
        store <- asks getStore
        lastApplied <- lift $ gets _lastApplied
        config <- ask
        liftIO $ takeSnapshotImpl (getRun config) lastApplied store

getValueImpl :: (Ord k) => k -> Store k v e -> IO (Maybe v)
getValueImpl k = fmap (getKey k) . readTVarIO . unStore

setValueImpl :: (Ord k, Storable v) => k -> v -> Store k v e -> IO ()
setValueImpl k v = modifyTVarIO (setKey k v) . unStore

replaceValueImpl :: (Ord k, Storable v)
                 => k
                 -> v
                 -> Store k v e
                 -> IO (Maybe CAS)
replaceValueImpl k v = stateTVarIO (replaceKey k v) . unStore

deleteValueImpl :: (Ord k) => k -> Store k v e -> IO ()
deleteValueImpl k = modifyTVarIO (deleteKey k) . unStore

cleanupExpiredImpl :: (Show k, Ord k, Storable v)
                   => Time
                   -> Store k v e
                   -> IO ()
cleanupExpiredImpl t = modifyTVarIO (cleanupStore t) . unStore

firstIndexImpl :: Store k v e -> IO LogIndex
firstIndexImpl = fmap (_lowIdx . _log) . readTVarIO . unStore

lastIndexImpl :: Store k v e -> IO LogIndex
lastIndexImpl = fmap (lastIndexLog . _log) . readTVarIO . unStore

loadEntryImpl :: LogIndex -> Store k v e -> IO (Maybe e)
loadEntryImpl (LogIndex k) =
    fmap (IM.lookup k . _entries . _log) . readTVarIO . unStore

termFromIndexImpl :: (Entry e) => LogIndex -> Store k v e -> IO (Maybe LogTerm)
termFromIndexImpl i = fmap (entryTermLog i . _log) . readTVarIO . unStore

storeEntriesImpl :: (Entry e, MonadIO m, SnapshotM s m, TakeSnapshotM m)
                 => [e]
                 -> Store k v e
                 -> m ()
storeEntriesImpl es store = do
    logSize <- liftIO $ persistAfter (modifyLog (storeEntriesLog es)) store
    snapshotInfo >>= \case
        Just (_, _, snapSize) | logSize > snapSize * 4 -> takeSnapshot
        _                                              -> return ()

deleteRangeImpl :: (Binary e)
                => LogIndex
                -> LogIndex
                -> Store k v e
                -> IO ()
deleteRangeImpl a b store =
    void $ persistAfter (modifyLog (deleteRangeLog a b)) store

applyEntryImpl :: (MonadIO m, StorageM k v m) => LogEntry k v -> m ()
applyEntryImpl LogEntry{ _data = entry, _completed = Completed lock } = do
    mapM_ (liftIO . atomically . flip putTMVar ()) lock
    applyStore entry
  where
    applyStore (Change _ k v) = setValue k v
    applyStore (Delete _ k)   = deleteValue k
    applyStore _              = return ()

loadSnapshotImpl :: (Ord k, Storable v)
                 => LogIndex
                 -> LogTerm
                 -> M.Map k v
                 -> Store k v e
                 -> IO ()
loadSnapshotImpl lastIndex lastTerm map (Store store) = atomically $
    modifyTVar' store (loadSnapshotStore lastIndex lastTerm map)

takeSnapshotImpl :: (KeyClass k, ValueClass v, Entry e)
                 => SnapFn k v
                 -> LogIndex
                 -> Store k v e
                 -> IO ()
takeSnapshotImpl run lastApplied store = do
    storeData <- readTVarIO $ unStore store
    let firstIndex = _lowIdx $ _log storeData
        lastTerm   = entryTerm . fromJust
                   . IM.lookup (unLogIndex lastApplied)
                   . _entries . _log
                   $ storeData
    forkIO $ run $ do
        createSnapshot lastApplied lastTerm
        let snapData = B.concat . BL.toChunks . encode . _map $ storeData
        -- FIXME(DarinM223): maybe write a version of writeSnapshot
        -- that takes in a lazy bytestring instead of a strict bytestring?
        writeSnapshot 0 snapData lastApplied
        saveSnapshot lastApplied

        snap <- readSnapshot lastApplied
        forM_ snap $ \snap ->
              liftIO
            $ flip persistAfter store
            $ loadSnapshotStore lastApplied lastTerm snap
            . modifyLog (deleteRangeLog firstIndex lastApplied)
    return ()
