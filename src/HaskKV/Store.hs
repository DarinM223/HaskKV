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
data Store k v e = Store
    { _map         :: M.Map k v
    , _heap        :: H.Heap (HeapVal k)
    , _log         :: Log e
    , _tempEntries :: [e]
    } deriving (Show)

class HasStoreTVar k v e r | r -> k v e where
    getStoreTVar :: r -> TVar (Store k v e)

newStoreValue :: Integer -> Int -> v -> IO (StoreValue v)
newStoreValue seconds version val = do
    currTime <- getCurrentTime
    let newTime = addUTCTime diff currTime
    return StoreValue
        { _version = version, _expireTime = Just newTime, _value = val }
  where
    diff = fromRational . toRational . secondsToDiffTime $ seconds

emptyStore :: Store k v e
emptyStore = Store { _map = M.empty
                   , _heap = H.empty
                   , _log = emptyLog
                   , _tempEntries = []
                   }

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
    , HasStoreTVar k v e r
    , KeyClass k
    , ValueClass v
    ) => StorageM k v (StoreT m) where

    getValue k = liftIO . getValueImpl k =<< asks getStoreTVar
    setValue k v = liftIO . setValueImpl k v =<< asks getStoreTVar
    replaceValue k v = liftIO . replaceValueImpl k v =<< asks getStoreTVar
    deleteValue k = liftIO . deleteValueImpl k =<< asks getStoreTVar
    cleanupExpired t = liftIO . cleanupExpiredImpl t =<< asks getStoreTVar

instance
    ( MonadIO m
    , MonadReader r m
    , HasStoreTVar k v e r
    , Entry e
    ) => LogM e (StoreT m) where

    firstIndex = liftIO . firstIndexImpl =<< asks getStoreTVar
    lastIndex = liftIO . lastIndexImpl =<< asks getStoreTVar
    loadEntry k = liftIO . loadEntryImpl k =<< asks getStoreTVar
    storeEntries es = liftIO . storeEntriesImpl es =<< asks getStoreTVar
    deleteRange a b = liftIO . deleteRangeImpl a b =<< asks getStoreTVar

instance
    ( MonadIO m
    , StorageM k v m
    , MonadReader r m
    , HasStoreTVar k v (LogEntry k v) r
    , KeyClass k
    , ValueClass v
    ) => ApplyEntryM k v (LogEntry k v) (StoreT m) where

    applyEntry = applyEntryImpl

getValueImpl :: (Ord k) => k -> TVar (Store k v e) -> IO (Maybe v)
getValueImpl k = fmap (getStore k) . readTVarIO

setValueImpl :: (Ord k, Storable v)
             => k
             -> v
             -> TVar (Store k v e)
             -> IO ()
setValueImpl k v = modifyTVarIO (setStore k v)

replaceValueImpl :: (Ord k, Storable v)
                 => k
                 -> v
                 -> TVar (Store k v e)
                 -> IO (Maybe CAS)
replaceValueImpl k v = stateTVarIO (replaceStore k v)

deleteValueImpl :: (Ord k) => k -> TVar (Store k v e) -> IO ()
deleteValueImpl k = modifyTVarIO (deleteStore k)

cleanupExpiredImpl :: (Show k, Ord k, Storable v)
                   => Time
                   -> TVar (Store k v e)
                   -> IO ()
cleanupExpiredImpl t = modifyTVarIO (cleanupStore t)

firstIndexImpl :: TVar (Store k v e) -> IO Int
firstIndexImpl = fmap (_lowIdx . _log) . readTVarIO

lastIndexImpl :: TVar (Store k v e) -> IO Int
lastIndexImpl = fmap (_highIdx . _log) . readTVarIO

loadEntryImpl :: Int -> TVar (Store k v e) -> IO (Maybe e)
loadEntryImpl k =
    fmap (IM.lookup k . _entries . _log) . readTVarIO

storeEntriesImpl :: (Entry e) => [e] -> TVar (Store k v e) -> IO ()
storeEntriesImpl es =
    modifyTVarIO (\s -> s { _log = storeEntriesLog es (_log s) })

deleteRangeImpl :: Int -> Int -> TVar (Store k v e) -> IO ()
deleteRangeImpl a b =
    modifyTVarIO (\s -> s { _log = deleteRangeLog a b (_log s) })

applyEntryImpl :: (MonadIO m, StorageM k v m) => LogEntry k v -> m ()
applyEntryImpl LogEntry{ _data = entry, _completed = Completed lock } = do
    mapM_ (liftIO . atomically . flip putTMVar ()) lock
    applyStore entry
  where
    applyStore (Change _ k v) = setValue k v
    applyStore (Delete _ k)   = deleteValue k
    applyStore _              = return ()

-- Pure store functions

getStore :: (Ord k) => k -> Store k v e -> Maybe v
getStore k s = _map s M.!? k

setStore :: (Ord k, Storable v) => k -> v -> Store k v e -> Store k v e
setStore k v s = s
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

replaceStore :: (Ord k, Storable v)
             => k
             -> v
             -> Store k v e
             -> (Maybe CAS, Store k v e)
replaceStore k v s
    | equalCAS  = (Just cas', s { _map = M.insert k v' $ _map s })
    | otherwise = (Nothing, s)
  where
    maybeV = _map s M.!? k
    equalCAS = fromMaybe True ((== version v) . version <$> maybeV)
    cas' = version v + 1
    v' = setVersion cas' v

deleteStore :: (Ord k) => k -> Store k v e -> Store k v e
deleteStore k s = s { _map = M.delete k $ _map s }

cleanupStore :: (Show k, Ord k, Storable v)
             => Time
             -> Store k v e
             -> Store k v e
cleanupStore curr s = case minHeapMaybe (_heap s) of
    Just (HeapVal (t, k)) | diff t curr <= 0 ->
        let (_, h') = fromJust . H.viewMin $ _heap s
            v       = getStore k s
        in
            -- Only delete key from store if it hasn't been
            -- replaced/removed after it was set.
            if maybe False ((== 0) . diff t) (v >>= expireTime)
                then
                      cleanupStore curr
                    . deleteStore k
                    $ s { _heap = h' }
                else
                    cleanupStore curr s { _heap = h' }
    _ -> s
  where
    diff a b = realToFrac (diffUTCTime a b)
