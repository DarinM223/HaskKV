{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module HaskKV.Store
    ( Storable (..)
    , StorageM (..)
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

class
    ( Monad s
    , Ord (Key s)
    , Storable (Value s)
    ) => StorageM s where

    type Key s   :: *
    type Value s :: *

    -- | Gets a value from the store given a key.
    getValue :: Key s -> s (Maybe (Value s))

    -- | Sets a value in the store given a key-value pairing.
    setValue :: Key s -> Value s -> s ()

    -- | Only sets the values if the CAS values match.
    --
    -- Returns the new CAS value if they match,
    -- Nothing otherwise.
    replaceValue :: Key s -> Value s -> s (Maybe CAS)

    -- | Deletes a value in the store given a key.
    deleteValue :: Key s -> s ()

    -- | Deletes all values that passed the expiration time.
    cleanupExpired :: Time -> s ()

-- | An in-memory storage implementation.
data MemStore k v = MemStore
    { memStoreMap  :: M.Map k v
    , memStoreHeap :: H.Heap (Time, k)
    } deriving (Show)

newtype MemStoreT k v m a = MemStoreT
    { unMemStoreT :: ReaderT (TVar (MemStore k v)) m a
    } deriving
        ( Functor, Applicative, Monad, MonadIO
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
    type Key (MemStoreT k v m) = k
    type Value (MemStoreT k v m) = v

    getValue k = return . getMemStore k =<< liftIO . readTVarIO =<< ask
    setValue k v = liftIO . modifyTVarIO (setMemStore k v) =<< ask
    deleteValue k = liftIO . modifyTVarIO (deleteMemStore k) =<< ask
    cleanupExpired t = liftIO . modifyTVarIO (cleanupMemStore t) =<< ask
    replaceValue k v = liftIO . stateTVarIO (replaceMemStore k v) =<< ask

checkAndSet :: (StorageM m) => Int -> Key m -> (Value m -> Value m) -> m Bool
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
