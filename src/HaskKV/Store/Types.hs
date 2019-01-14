module HaskKV.Store.Types where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Aeson (FromJSON, ToJSON)
import Data.Binary
import Data.Binary.Orphans ()
import Data.Maybe (fromMaybe)
import Data.Time
import GHC.Generics
import HaskKV.Log.Class
import HaskKV.Log.InMem
import HaskKV.Types

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

data StorageM k v m = StorageM
  { getValue :: k -> m (Maybe v)
  -- ^ Gets a value from the store given a key.
  , setValue :: k -> v -> m ()
  -- ^ Sets a value in the store given a key-value pairing.
  , replaceValue :: k -> v -> m (Maybe CAS)
  -- ^ Only sets the values if the CAS values match.
  --
  -- Returns the new CAS value if they match,
  -- Nothing otherwise.
  , deleteValue :: k -> m ()
  -- ^ Deletes a value in the store given a key.
  , cleanupExpired :: Time -> m ()
  -- ^ Deletes all values that passed the expiration time.
  }

newtype LoadSnapshotM s m = LoadSnapshotM
  { loadSnapshot :: LogIndex -> LogTerm -> s -> m () }
newtype ApplyEntryM e m = ApplyEntryM { applyEntry :: e -> m () }
newtype TakeSnapshotM m = TakeSnapshotM { takeSnapshot :: m () }

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
  , _sid         :: SID
  } deriving (Show)

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
  { _map         = M.empty
  , _heap        = H.empty
  , _log         = fromMaybe emptyLog log
  , _tempEntries = []
  , _sid         = sid
  }

class HasStorageM k v m cfg | cfg -> k v m where
  getStorageM :: cfg -> StorageM k v m
class HasLoadSnapshotM s m cfg | cfg -> s m where
  getLoadSnapshotM :: cfg -> LoadSnapshotM s m
class HasApplyEntryM e m cfg | cfg -> e m where
  getApplyEntryM :: cfg -> ApplyEntryM e m
class HasTakeSnapshotM m cfg | cfg -> m where
  getTakeSnapshotM :: cfg -> TakeSnapshotM m
class HasStore k v e cfg | cfg -> k v e where
  getStore :: cfg -> Store k v e
