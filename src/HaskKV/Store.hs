{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module HaskKV.Store where

import Control.Monad.State
import Data.Time

import qualified Data.Map as M
import qualified Data.Heap as H

type Time = UTCTime

class Storable v where
    expireTime :: v -> Time
    version    :: v -> Int

class ( Monad s
      , Ord (Key s)
      , Storable (Value s)
      ) => StorageM s where
    type Key s   :: *
    type Value s :: *

    -- | Gets a value from the store given a key.
    getValue :: Key s -> s (Maybe (Value s))

    -- | Sets a value in the store given a key-value pairing.
    setValue :: Key s -> Value s -> s ()

    -- TODO(DarinM223): implement this later.
    checkAndSetValue :: Key s -> Value s -> s ()

    -- | Deletes a value in the store given a key.
    deleteValue :: Key s -> s ()

    -- | Deletes all values that passed the expiration time.
    cleanupExpired :: Time -> s ()

-- | An in-memory storage implementation.
data MemStore k v = MemStore
    { memStoreMap  :: M.Map k v
    , memStoreHeap :: H.Heap (Time, k)
    } deriving (Show)

getMemStore :: (Ord k) => k -> MemStore k v -> Maybe v
getMemStore k s = memStoreMap s M.!? k

setMemStore :: (Ord k, Storable v) => k -> v -> MemStore k v -> MemStore k v
setMemStore k v s = s { memStoreMap = M.insert k v $ memStoreMap s }

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
                then cleanupMemStore curr $ deleteMemStore k s { memStoreHeap = h' }
                else cleanupMemStore curr s { memStoreHeap = h' }
    Nothing -> s
  where
    diff a b = realToFrac (diffUTCTime a b)
    updateHeap h s = s { memStoreHeap = h }

minHeapMaybe :: H.Heap a -> Maybe a
minHeapMaybe h
    | H.null h  = Nothing
    | otherwise = Just $ H.minimum h

-- TODO(DarinM223): use locks instead.
newtype MemStoreT k v m a = MemStoreT
    { unMemStoreT :: StateT (MemStore k v) m a
    } deriving (Functor, Applicative, Monad, MonadState (MemStore k v))

instance (Monad m, Ord k, Storable v) => StorageM (MemStoreT k v m) where
    type Key (MemStoreT k v m) = k
    type Value (MemStoreT k v m) = v

    getValue k = return . getMemStore k =<< get
    setValue k v = modify $ setMemStore k v
    deleteValue k = modify $ deleteMemStore k
    cleanupExpired t = modify $ cleanupMemStore t

    checkAndSetValue = undefined

data Message k v
    = Get k
    | Set k v Time
    | Cas k v Time Int
    | Delete k
    deriving (Show, Eq)
