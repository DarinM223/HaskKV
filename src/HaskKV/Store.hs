{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Store where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Maybe (fromJust)
import Data.Time
import HaskKV.Utils

import qualified Data.Map as M
import qualified Data.Heap as H

type Time = UTCTime
type CAS  = Int

class Storable v where
    expireTime :: v -> Time
    version    :: v -> CAS
    setVersion :: CAS -> v -> v

class (Monad s, Ord k, Storable v) => StorageM k v s | s -> k v where
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

    default getValue :: (MonadTrans t, StorageM k v s', s ~ t s')
                     => k
                     -> s (Maybe v)
    getValue = lift . getValue

    default setValue :: (MonadTrans t, StorageM k v s', s ~ t s')
                     => k
                     -> v
                     -> s ()
    setValue k v = lift $ setValue k v

    default replaceValue :: (MonadTrans t, StorageM k v s', s ~ t s')
                         => k
                         -> v
                         -> s (Maybe CAS)
    replaceValue k v = lift $ replaceValue k v

    default deleteValue :: (MonadTrans t, StorageM k v s', s ~ t s') => k -> s ()
    deleteValue = lift . deleteValue

    default cleanupExpired :: (MonadTrans t, StorageM k v s', s ~ t s')
                           => Time
                           -> s ()
    cleanupExpired = lift . cleanupExpired


-- | An in-memory storage implementation.
data Store k v = Store
    { _map  :: M.Map k v
    , _heap :: H.Heap (Time, k)
    } deriving (Show)

newtype StoreT k v m a = StoreT
    { unStoreT :: ReaderT (TVar (Store k v)) m a
    } deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (TVar (Store k v))
        )

execStoreT :: (MonadIO m) => StoreT k v m b -> Store k v -> m b
execStoreT (StoreT (ReaderT f)) = f <=< liftIO . newTVarIO

execStoreTVar :: StoreT k v m b -> TVar (Store k v) -> m b
execStoreTVar (StoreT (ReaderT f)) = f

instance (MonadIO m, Ord k, Storable v) => StorageM k v (StoreT k v m) where
    getValue k = return . getStore k =<< liftIO . readTVarIO =<< ask
    setValue k v = liftIO . modifyTVarIO (setStore k v) =<< ask
    replaceValue k v = liftIO . stateTVarIO (replaceStore k v) =<< ask
    deleteValue k = liftIO . modifyTVarIO (deleteStore k) =<< ask
    cleanupExpired t = liftIO . modifyTVarIO (cleanupStore t) =<< ask

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

getStore :: (Ord k) => k -> Store k v -> Maybe v
getStore k s = _map s M.!? k

setStore :: (Ord k, Storable v) => k -> v -> Store k v -> Store k v
setStore k v s = s { _map = M.insert k v' $ _map s }
  where
    maybeV = _map s M.!? k
    maybeCas' = (+ 1) . version <$> maybeV
    v' = maybe v id (setVersion <$> maybeCas' <*> pure v)

replaceStore :: (Ord k, Storable v)
             => k
             -> v
             -> Store k v
             -> (Maybe CAS, Store k v)
replaceStore k v s
    | equalCAS  = (Just cas', s { _map = M.insert k v' $ _map s })
    | otherwise = (Nothing, s)
  where
    maybeV = _map s M.!? k
    equalCAS = maybe True id ((== version v) . version <$> maybeV)
    cas' = version v + 1
    v' = setVersion cas' v

deleteStore :: (Ord k) => k -> Store k v -> Store k v
deleteStore k s = s { _map = M.delete k $ _map s }

cleanupStore :: (Ord k, Storable v) => Time -> Store k v -> Store k v
cleanupStore curr s = case minHeapMaybe (_heap s) of
    Just (t, k) | diff t curr <= 0 ->
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
