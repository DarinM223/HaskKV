{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Store where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Aeson
import Data.Binary
import Data.Binary.Orphans ()
import Data.Maybe (fromJust, fromMaybe)
import Data.Time
import GHC.Generics
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Utils

import qualified Data.IntMap as IM
import qualified Data.Map as M
import qualified Data.Heap as H

type Time = UTCTime
type CAS  = Int

class Storable v where
    expireTime :: v -> Maybe Time
    version    :: v -> CAS
    setVersion :: CAS -> v -> v

type KeyClass k = (Show k, Ord k, Binary k)
type ValueClass v = (Show v, Storable v, Binary v)

class (Monad s, KeyClass k, ValueClass v) => StorageM k v s | s -> k v where
    -- | Gets a value from the store given a key.
    getValue :: k -> s (Maybe v)

    -- | Sets a value in the store given a key-value pairing.
    setValue :: k -> v -> s ()

    -- | Only sets the values if the CAS values match.
    --
    -- Returns the new CAS value if they match,
    -- Nothing otherwise.
    replaceValue :: k -> v -> s (Maybe CAS)

    -- | Deletes a value in the store given a key.
    deleteValue :: k -> s ()

    -- | Deletes all values that passed the expiration time.
    cleanupExpired :: Time -> s ()

class (Binary s) => LoadSnapshotM s m | m -> s where
    loadSnapshot :: s -> m ()

class (StorageM k v m, LogM e m) => ApplyEntryM k v e m | m -> k v e where
    -- | Applies a log entry.
    applyEntry :: e -> m ()

data StoreValue v = StoreValue
    { _expireTime :: Maybe Time
    , _version    :: CAS
    , _value      :: v
    } deriving (Show, Eq, Generic)

instance (Binary v) => Binary (StoreValue v)
instance (ToJSON v) => ToJSON (StoreValue v)
instance (FromJSON v) => FromJSON (StoreValue v)

instance Storable (StoreValue v) where
    expireTime = _expireTime
    version = _version
    setVersion cas s = s { _version = cas }

newtype HeapVal k = HeapVal (Time, k) deriving (Show, Eq)
instance (Eq k) => Ord (HeapVal k) where
    compare (HeapVal a) (HeapVal b) = compare (fst a) (fst b)

-- | An in-memory storage implementation.
data StoreData k v e = StoreData
    { _map         :: M.Map k v
    , _heap        :: H.Heap (HeapVal k)
    , _log         :: Log e
    , _tempEntries :: [e]
    } deriving (Show)

newtype Store k v e = Store { unStore :: TVar (StoreData k v e) }

class HasStore k v e r | r -> k v e where
    getStore :: r -> Store k v e

newStoreValue :: Integer -> Int -> v -> IO (StoreValue v)
newStoreValue seconds version val = do
    currTime <- getCurrentTime
    let newTime = addUTCTime diff currTime
    return StoreValue
        { _version = version, _expireTime = Just newTime, _value = val }
  where
    diff = fromRational . toRational . secondsToDiffTime $ seconds

emptyStore :: IO (Store k v e)
emptyStore = do
    store <- newTVarIO StoreData { _map = M.empty
                                 , _heap = H.empty
                                 , _log = emptyLog
                                 , _tempEntries = []
                                 }
    return $ Store store

checkAndSet :: (StorageM k v m) => Int -> k -> (v -> v) -> m Bool
checkAndSet attempts k f
    | attempts == 0 = return False
    | otherwise = do
        maybeV <- getValue k
        result <- mapM (replaceValue k) . fmap f $ maybeV
        case result of
            Just (Just _) -> return True
            Just Nothing  -> checkAndSet (attempts - 1) k f
            _             -> return False

newtype StoreT m a = StoreT { unStoreT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance
    ( MonadIO m
    , MonadReader r m
    , HasStore k v e r
    , KeyClass k
    , ValueClass v
    ) => StorageM k v (StoreT m) where

    getValue k = liftIO . getValueImpl k =<< asks getStore
    setValue k v = liftIO . setValueImpl k v =<< asks getStore
    replaceValue k v = liftIO . replaceValueImpl k v =<< asks getStore
    deleteValue k = liftIO . deleteValueImpl k =<< asks getStore
    cleanupExpired t = liftIO . cleanupExpiredImpl t =<< asks getStore

instance
    ( MonadIO m
    , MonadReader r m
    , HasStore k v e r
    , Entry e
    ) => LogM e (StoreT m) where

    firstIndex = liftIO . firstIndexImpl =<< asks getStore
    lastIndex = liftIO . lastIndexImpl =<< asks getStore
    loadEntry k = liftIO . loadEntryImpl k =<< asks getStore
    storeEntries es = liftIO . storeEntriesImpl es =<< asks getStore
    deleteRange a b = liftIO . deleteRangeImpl a b =<< asks getStore

instance
    ( MonadIO m
    , StorageM k v m
    , MonadReader r m
    , HasStore k v (LogEntry k v) r
    , KeyClass k
    , ValueClass v
    ) => ApplyEntryM k v (LogEntry k v) (StoreT m) where

    applyEntry = applyEntryImpl

instance
    ( MonadIO m
    , MonadReader r m
    , HasStore k v e r
    , KeyClass k
    , ValueClass v
    ) => LoadSnapshotM (M.Map k v) (StoreT m) where

    loadSnapshot map = liftIO . loadSnapshotImpl map =<< asks getStore

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

firstIndexImpl :: Store k v e -> IO Int
firstIndexImpl = fmap (_lowIdx . _log) . readTVarIO . unStore

lastIndexImpl :: Store k v e -> IO Int
lastIndexImpl = fmap (_highIdx . _log) . readTVarIO . unStore

loadEntryImpl :: Int -> Store k v e -> IO (Maybe e)
loadEntryImpl k = fmap (IM.lookup k . _entries . _log) . readTVarIO . unStore

storeEntriesImpl :: (Entry e) => [e] -> Store k v e -> IO ()
storeEntriesImpl es
    = modifyTVarIO (\s -> s { _log = storeEntriesLog es (_log s) })
    . unStore

deleteRangeImpl :: Int -> Int -> Store k v e -> IO ()
deleteRangeImpl a b
    = modifyTVarIO (\s -> s { _log = deleteRangeLog a b (_log s) })
    . unStore

applyEntryImpl :: (MonadIO m, StorageM k v m) => LogEntry k v -> m ()
applyEntryImpl LogEntry{ _data = entry, _completed = Completed lock } = do
    mapM_ (liftIO . atomically . flip putTMVar ()) lock
    applyStore entry
  where
    applyStore (Change _ k v) = setValue k v
    applyStore (Delete _ k)   = deleteValue k
    applyStore _              = return ()

loadSnapshotImpl :: (Ord k, Storable v) => M.Map k v -> Store k v e -> IO ()
loadSnapshotImpl map (Store store) = atomically $
    forM_ (M.assocs map) $ \(k, v) ->
        modifyTVar' store (setKey k v)

-- Pure store functions

getKey :: (Ord k) => k -> StoreData k v e -> Maybe v
getKey k s = _map s M.!? k

setKey :: (Ord k, Storable v) => k -> v -> StoreData k v e -> StoreData k v e
setKey k v s = s
    { _map  = M.insert k v' $ _map s
    , _heap = heap'
    }
  where
    maybeV = _map s M.!? k
    maybeCas' = (+ 1) . version <$> maybeV
    v' = fromMaybe v (setVersion <$> maybeCas' <*> pure v)
    heap' = case expireTime v' of
        Just time -> H.insert (HeapVal (time, k)) $ _heap s
        _         -> _heap s

replaceKey :: (Ord k, Storable v)
           => k
           -> v
           -> StoreData k v e
           -> (Maybe CAS, StoreData k v e)
replaceKey k v s
    | equalCAS  = (Just cas', s { _map = M.insert k v' $ _map s })
    | otherwise = (Nothing, s)
  where
    maybeV = _map s M.!? k
    equalCAS = fromMaybe True ((== version v) . version <$> maybeV)
    cas' = version v + 1
    v' = setVersion cas' v

deleteKey :: (Ord k) => k -> StoreData k v e -> StoreData k v e
deleteKey k s = s { _map = M.delete k $ _map s }

cleanupStore :: (Show k, Ord k, Storable v)
             => Time
             -> StoreData k v e
             -> StoreData k v e
cleanupStore curr s = case minHeapMaybe (_heap s) of
    Just (HeapVal (t, k)) | diff t curr <= 0 ->
        let (_, h') = fromJust . H.viewMin $ _heap s
            v       = getKey k s
        in
            -- Only delete key from store if it hasn't been
            -- replaced/removed after it was set.
            if maybe False ((== 0) . diff t) (v >>= expireTime)
                then
                      cleanupStore curr
                    . deleteKey k
                    $ s { _heap = h' }
                else
                    cleanupStore curr s { _heap = h' }
    _ -> s
  where
    diff a b = realToFrac (diffUTCTime a b)
