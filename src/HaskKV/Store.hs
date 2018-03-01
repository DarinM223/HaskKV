{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module HaskKV.Store
    ( Storable (..)
    , StorageM (..)
    , StorageMK (..)
    , StorageMKV (..)
    , MemStore (..)
    , MemStoreT (..)
    , execMemStoreT
    , execMemStoreTVar
    , checkAndSet
    ) where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Time

import qualified Data.Map as M
import qualified Data.Heap as H

type Time = UTCTime
type CAS  = Int

class Storable v where
    expireTime :: v -> Time
    version    :: v -> CAS
    setVersion :: CAS -> v -> v

class (Monad s) => StorageM s where
    -- | Deletes all values that passed the expiration time.
    cleanupExpired :: Time -> s ()

class (StorageM s, Ord k) => StorageMK k s where
    -- | Deletes a value in the store given a key.
    deleteValue :: k -> s ()

class (StorageMK k s, Storable v) => StorageMKV k v s where
    -- | Gets a value from the store given a key.
    getValue :: k -> s (Maybe v)

    -- | Sets a value in the store given a key-value pairing.
    setValue :: k -> v -> s ()

    -- | Only sets the values if the CAS values match.
    --
    -- Returns the new CAS value if they match,
    -- Nothing otherwise.
    replaceValue :: k -> v -> s (Maybe CAS)


-- | An in-memory storage implementation.
data MemStore k v = MemStore
    { memStoreMap  :: M.Map k v
    , memStoreHeap :: H.Heap (Time, k)
    } deriving (Show)

newtype MemStoreT k v m a = MemStoreT
    { unMemStoreT :: ReaderT (TVar (MemStore k v)) m a
    } deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (TVar (MemStore k v))
        )

execMemStoreT :: (MonadIO m) => MemStoreT k v m b -> MemStore k v -> m b
execMemStoreT (MemStoreT (ReaderT f)) = f <=< liftIO . newTVarIO

execMemStoreTVar :: (MonadIO m)
                 => MemStoreT k v m b
                 -> TVar (MemStore k v)
                 -> m b
execMemStoreTVar (MemStoreT (ReaderT f)) = f

instance (MonadIO m, Ord k, Storable v) => StorageM (MemStoreT k v m) where
    cleanupExpired t = liftIO . modifyTVarIO (cleanupMemStore t) =<< ask

instance (MonadIO m, Ord k, Storable v) => StorageMK k (MemStoreT k v m) where
    deleteValue k = liftIO . modifyTVarIO (deleteMemStore k) =<< ask

instance (MonadIO m, Ord k, Storable v) => StorageMKV k v (MemStoreT k v m) where
    getValue k = return . getMemStore k =<< liftIO . readTVarIO =<< ask
    setValue k v = liftIO . modifyTVarIO (setMemStore k v) =<< ask
    replaceValue k v = liftIO . stateTVarIO (replaceMemStore k v) =<< ask

instance (StorageM m) => StorageM (ReaderT r m) where
    cleanupExpired = lift . cleanupExpired

instance (StorageMK k m) => StorageMK k (ReaderT r m) where
    deleteValue = lift . deleteValue

instance (StorageMKV k v m) => StorageMKV k v (ReaderT r m) where
    getValue = lift . getValue
    setValue k v = lift $ setValue k v
    replaceValue k v = lift $ replaceValue k v

checkAndSet :: (StorageMKV k v m) => Int -> k -> (v -> v) -> m Bool
checkAndSet attempts k f
    | attempts == 0 = return False
    | otherwise = do
        maybeV <- getValue k
        result <- mapM (replaceValue k) . fmap f $ maybeV
        case result of
            Just (Just _) -> return True
            Just Nothing  -> checkAndSet (attempts - 1) k f
            _             -> return False

stateTVarIO :: (s -> (a, s)) -> TVar s -> IO a
stateTVarIO f v = atomically $ do
    s <- readTVar v
    let (r, s') = f s
    writeTVar v s'
    return r

modifyTVarIO :: (a -> a) -> TVar a -> IO ()
modifyTVarIO f v = atomically $ modifyTVar v f

getMemStore :: (Ord k) => k -> MemStore k v -> Maybe v
getMemStore k s = memStoreMap s M.!? k

setMemStore :: (Ord k, Storable v) => k -> v -> MemStore k v -> MemStore k v
setMemStore k v s = s { memStoreMap = M.insert k v' $ memStoreMap s }
  where
    maybeV = memStoreMap s M.!? k
    maybeCas' = (+ 1) . version <$> maybeV
    v' = maybe v id (setVersion <$> maybeCas' <*> pure v)

replaceMemStore :: (Ord k, Storable v)
                => k
                -> v
                -> MemStore k v
                -> (Maybe CAS, MemStore k v)
replaceMemStore k v s
    | equalCAS  = (Just cas', s { memStoreMap = M.insert k v' $ memStoreMap s })
    | otherwise = (Nothing, s)
  where
    maybeV = memStoreMap s M.!? k
    equalCAS = maybe True id ((== version v) . version <$> maybeV)
    cas' = version v + 1
    v' = setVersion cas' v

deleteMemStore :: (Ord k) => k -> MemStore k v -> MemStore k v
deleteMemStore k s = s { memStoreMap = M.delete k $ memStoreMap s }

cleanupMemStore :: (Ord k, Storable v) => Time -> MemStore k v -> MemStore k v
cleanupMemStore curr s = case minHeapMaybe (memStoreHeap s) of
    Just (t, k) | diff t curr <= 0 ->
        let Just (_, h') = H.viewMin (memStoreHeap s)
            v            = getMemStore k s
        in
            -- Only delete key from store if it hasn't been
            -- replaced/removed after it was set.
            if maybe False ((== 0) . diff t . expireTime) v
                then
                    cleanupMemStore curr
                    . deleteMemStore k
                    $ s { memStoreHeap = h' }
                else
                    cleanupMemStore curr s { memStoreHeap = h' }
    Nothing -> s
  where
    diff a b = realToFrac (diffUTCTime a b)

minHeapMaybe :: H.Heap a -> Maybe a
minHeapMaybe h
    | H.null h  = Nothing
    | otherwise = Just $ H.minimum h
