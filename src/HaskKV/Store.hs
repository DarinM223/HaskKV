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

class HasStore k v e c | c -> k v e where
    getStore :: c -> TVar (Store k v e)

instance HasStore k v e (TVar (Store k v e)) where
    getStore = id

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

-- StorageM instance implementations

getValueImpl :: (Ord k, MonadIO m, MonadReader c m, HasStore k v e c)
             => k
             -> m (Maybe v)
getValueImpl k = fmap (storeGetKey k)
               . liftIO
               . readTVarIO
             =<< fmap getStore ask

setValueImpl :: ( Ord k
                , Storable v
                , MonadIO m
                , MonadReader c m
                , HasStore k v e c
                )
             => k
             -> v
             -> m ()
setValueImpl k v = liftIO
                 . modifyTVarIO (storeSetValue k v)
               =<< fmap getStore ask

replaceValueImpl :: ( Ord k
                    , Storable v
                    , MonadIO m
                    , MonadReader c m
                    , HasStore k v e c
                    )
                 => k
                 -> v
                 -> m (Maybe CAS)
replaceValueImpl k v = liftIO
                     . stateTVarIO (storeReplaceValue k v)
                   =<< fmap getStore ask

deleteValueImpl :: (Ord k, MonadIO m, MonadReader c m, HasStore k v e c)
                => k
                -> m ()
deleteValueImpl k = liftIO
                  . modifyTVarIO (storeDeleteKey k)
                =<< fmap getStore ask

cleanupExpiredImpl :: ( Show k
                      , Ord k
                      , Storable v
                      , MonadIO m
                      , MonadReader c m
                      , HasStore k v e c
                      )
                   => Time
                   -> m ()
cleanupExpiredImpl t = liftIO
                     . modifyTVarIO (storeCleanupExpired t)
                   =<< fmap getStore ask

-- LogM  instance implementations

firstIndexImpl :: (MonadIO m, MonadReader c m, HasStore k v e c) => m Int
firstIndexImpl = fmap (_lowIdx . _log)
               . liftIO
               . readTVarIO
             =<< fmap getStore ask

lastIndexImpl :: (MonadIO m, MonadReader c m, HasStore k v e c) => m Int
lastIndexImpl = fmap (_highIdx . _log)
              . liftIO
              . readTVarIO
            =<< fmap getStore ask

loadEntryImpl :: (MonadIO m, MonadReader c m, HasStore k v e c)
              => Int
              -> m (Maybe e)
loadEntryImpl k = fmap (IM.lookup k . _entries . _log)
                . liftIO
                . readTVarIO
              =<< fmap getStore ask

storeEntriesImpl :: (MonadIO m, Entry e, MonadReader c m, HasStore k v e c)
                 => [e]
                 -> m ()
storeEntriesImpl es
    = liftIO
    . modifyTVarIO (\s -> s { _log = storeEntriesLog es (_log s) })
  =<< fmap getStore ask

deleteRangeImpl :: (MonadIO m, MonadReader c m, HasStore k v e c)
                => Int
                -> Int
                -> m ()
deleteRangeImpl a b
    = liftIO
    . modifyTVarIO (\s -> s { _log = deleteRangeLog a b (_log s) })
  =<< fmap getStore ask

-- ApplyEntryM instance implementations

applyEntryImpl :: (MonadIO m, StorageM k v m) => LogEntry k v -> m ()
applyEntryImpl LogEntry{ _data = entry, _completed = Completed lock } = do
    mapM_ (liftIO . atomically . flip putTMVar ()) lock
    applyStore entry
  where
    applyStore (Change _ k v) = setValue k v
    applyStore (Delete _ k)   = deleteValue k
    applyStore _              = return ()

-- Pure store functions

storeGetKey :: (Ord k) => k -> Store k v e -> Maybe v
storeGetKey k s = _map s M.!? k

storeSetValue :: (Ord k, Storable v) => k -> v -> Store k v e -> Store k v e
storeSetValue k v s = s
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

storeReplaceValue :: (Ord k, Storable v)
                  => k
                  -> v
                  -> Store k v e
                  -> (Maybe CAS, Store k v e)
storeReplaceValue k v s
    | equalCAS  = (Just cas', s { _map = M.insert k v' $ _map s })
    | otherwise = (Nothing, s)
  where
    maybeV = _map s M.!? k
    equalCAS = fromMaybe True ((== version v) . version <$> maybeV)
    cas' = version v + 1
    v' = setVersion cas' v

storeDeleteKey :: (Ord k) => k -> Store k v e -> Store k v e
storeDeleteKey k s = s { _map = M.delete k $ _map s }

storeCleanupExpired :: (Show k, Ord k, Storable v)
                    => Time
                    -> Store k v e
                    -> Store k v e
storeCleanupExpired curr s = case minHeapMaybe (_heap s) of
    Just (HeapVal (t, k)) | diff t curr <= 0 ->
        let (_, h') = fromJust . H.viewMin $ _heap s
            v       = storeGetKey k s
        in
            -- Only delete key from store if it hasn't been
            -- replaced/removed after it was set.
            if maybe False ((== 0) . diff t) (v >>= expireTime)
                then
                      storeCleanupExpired curr
                    . storeDeleteKey k
                    $ s { _heap = h' }
                else
                    storeCleanupExpired curr s { _heap = h' }
    _ -> s
  where
    diff a b = realToFrac (diffUTCTime a b)
