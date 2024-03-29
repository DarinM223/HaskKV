{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FunctionalDependencies #-}
module HaskKV.Store.Types where

import Control.Concurrent.STM (TVar, newTVarIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.Binary (Binary)
import Data.Binary.Instances ()
import Data.Maybe (fromMaybe)
import Data.Time (UTCTime, secondsToDiffTime, addUTCTime, getCurrentTime)
import GHC.Generics (Generic)
import HaskKV.Log.Class (LogM)
import HaskKV.Log.InMem (Log, emptyLog)
import HaskKV.Types (LogIndex, LogTerm, SID)

import qualified Data.Heap as H
import qualified Data.Map as M

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
  loadSnapshot :: LogIndex -> LogTerm -> s -> m ()

class (StorageM k v m, LogM e m) => ApplyEntryM k v e m | m -> k v e where
  -- | Applies a log entry.
  applyEntry :: e -> m ()

class TakeSnapshotM m where
  takeSnapshot :: m ()

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
  { map         :: M.Map k v
  , heap        :: H.Heap (HeapVal k)
  , log         :: Log e
  , tempEntries :: [e]
  , sid         :: SID
  } deriving (Show, Generic)

newtype Store k v e = Store { unStore :: TVar (StoreData k v e) }

newStoreValue :: Integer -> Int -> v -> IO (StoreValue v)
newStoreValue seconds version val = do
  currTime <- getCurrentTime
  let newTime = addUTCTime diff currTime
  return StoreValue
    { _version    = version
    , _expireTime = Just newTime
    , _value      = val
    }
  where diff = fromRational . toRational . secondsToDiffTime $ seconds

newStore :: SID -> Maybe (Log e) -> IO (Store k v e)
newStore sid log = fmap Store . newTVarIO $ newStoreData sid log

newStoreData :: SID -> Maybe (Log e) -> StoreData k v e
newStoreData sid log = StoreData
  { map         = M.empty
  , heap        = H.empty
  , log         = fromMaybe emptyLog log
  , tempEntries = []
  , sid         = sid
  }
