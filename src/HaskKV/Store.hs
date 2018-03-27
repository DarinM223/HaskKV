{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Store where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Maybe (fromJust)
import Data.Time
import HaskKV.Log
import HaskKV.Utils

import qualified Data.IntMap as IM
import qualified Data.Map as M
import qualified Data.Heap as H

type Time = UTCTime
type CAS  = Int

class Storable v where
    expireTime :: v -> Time
    version    :: v -> CAS
    setVersion :: CAS -> v -> v

class (Monad s, Show k, Ord k, Storable v) => StorageM k v s | s -> k v where
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

    default getValue :: (MonadTrans t, StorageM k v s', s ~ t s') => k -> s (Maybe v)
    default setValue :: (MonadTrans t, StorageM k v s', s ~ t s') => k -> v -> s ()
    default replaceValue :: (MonadTrans t, StorageM k v s', s ~ t s') => k -> v -> s (Maybe CAS)
    default deleteValue :: (MonadTrans t, StorageM k v s', s ~ t s') => k -> s ()
    default cleanupExpired :: (MonadTrans t, StorageM k v s', s ~ t s') => Time -> s ()

    getValue = lift . getValue
    setValue k v = lift $ setValue k v
    replaceValue k v = lift $ replaceValue k v
    deleteValue = lift . deleteValue
    cleanupExpired = lift . cleanupExpired

data StoreValue v = StoreValue
    { _expireTime :: Time
    , _version    :: CAS
    , _value      :: v
    } deriving (Show, Eq)

instance Storable (StoreValue v) where
    expireTime = _expireTime
    version = _version
    setVersion cas s = s { _version = cas }

newtype HeapVal k = HeapVal (Time, k) deriving (Show, Eq)
instance (Eq k) => Ord (HeapVal k) where
    compare (HeapVal a) (HeapVal b) = compare (fst a) (fst b)

-- | An in-memory storage implementation.
data Store k v e = Store
    { _map  :: M.Map k v
    , _heap :: H.Heap (HeapVal k)
    , _log  :: Log e
    } deriving (Show)

newtype StoreT k v e m a = StoreT
    { unStoreT :: ReaderT (TVar (Store k v e)) m a
    } deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (TVar (Store k v e))
        )

runStoreT :: (MonadIO m) => StoreT k v e m b -> Store k v e -> m b
runStoreT (StoreT (ReaderT f)) = f <=< liftIO . newTVarIO

runStoreTVar :: StoreT k v e m b -> TVar (Store k v e) -> m b
runStoreTVar (StoreT (ReaderT f)) = f

instance
    ( MonadIO m
    , Ord k
    , Show k
    , Storable v
    ) => StorageM k v (StoreT k v e m) where

    getValue k = return . getStore k =<< liftIO . readTVarIO =<< ask
    setValue k v = liftIO . modifyTVarIO (setStore k v) =<< ask
    replaceValue k v = liftIO . stateTVarIO (replaceStore k v) =<< ask
    deleteValue k = liftIO . modifyTVarIO (deleteStore k) =<< ask
    cleanupExpired t = liftIO . modifyTVarIO (cleanupStore t) =<< ask

instance (MonadIO m, Entry e) => LogM e (StoreT k v e m) where
    firstIndex = return . _lowIdx . _log =<< liftIO . readTVarIO =<< ask
    lastIndex = return . _highIdx . _log =<< liftIO . readTVarIO =<< ask
    loadEntry k =
        return . IM.lookup k . _entries . _log =<< liftIO . readTVarIO =<< ask
    storeEntries es
        = liftIO
        . modifyTVarIO (\s -> s { _log = storeEntriesLog es (_log s) })
      =<< ask
    deleteRange a b
        = liftIO
        . modifyTVarIO (\s -> s { _log = deleteRangeLog a b (_log s) })
      =<< ask

instance (StorageM k v m) => StorageM k v (ReaderT r m)

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

createStoreValue :: Integer -> Int -> v -> IO (StoreValue v)
createStoreValue seconds version val = do
    currTime <- getCurrentTime
    let newTime = addUTCTime diff currTime
    return $ StoreValue
        { _version = version, _expireTime = newTime, _value = val }
  where
    diff = fromRational . toRational . secondsToDiffTime $ seconds

emptyStore :: Store k v e
emptyStore = Store { _map = M.empty, _heap = H.empty, _log = emptyLog }

getStore :: (Ord k) => k -> Store k v e -> Maybe v
getStore k s = _map s M.!? k

setStore :: (Ord k, Storable v) => k -> v -> Store k v e -> Store k v e
setStore k v s = s
    { _map  = M.insert k v' $ _map s
    , _heap = H.insert (HeapVal (time, k)) $ _heap s
    }
  where
    maybeV = _map s M.!? k
    maybeCas' = (+ 1) . version <$> maybeV
    v' = maybe v id (setVersion <$> maybeCas' <*> pure v)
    time = expireTime v'

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
    equalCAS = maybe True id ((== version v) . version <$> maybeV)
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
            if maybe False ((== 0) . diff t . expireTime) v
                then
                      cleanupStore curr
                    . deleteStore k
                    $ s { _heap = h' }
                else
                    cleanupStore curr s { _heap = h' }
    _ -> s
  where
    diff a b = realToFrac (diffUTCTime a b)
