module HaskKV.Log where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import Data.List
import GHC.Generics
import HaskKV.Store
import HaskKV.Utils

import qualified Data.IntMap as IM

class (Binary l) => Entry l where
    entryIndex :: l -> Int
    entryTerm  :: l -> Int

class (Monad m) => LogM m where
    -- | Returns the first index written.
    firstIndex :: m Int

    -- | Returns the last index written.
    lastIndex :: m Int

    -- | Deletes entries in an inclusive range.
    deleteRange :: Int -> Int -> m ()

    default firstIndex :: (MonadTrans t, LogM m', m ~ t m') => m Int
    firstIndex = lift firstIndex

    default lastIndex :: (MonadTrans t, LogM m', m ~ t m') => m Int
    lastIndex = lift lastIndex

    default deleteRange :: (MonadTrans t, LogM m', m ~ t m')
                        => Int
                        -> Int
                        -> m ()
    deleteRange a b = lift $ deleteRange a b

class (LogM m, Entry e) => LogME e m where
    -- | Gets a log entry at the specified index.
    loadEntry :: Int -> m (Maybe e)

    -- | Stores multiple log entries.
    storeEntries :: [e] -> m ()

    default loadEntry :: (MonadTrans t, LogME e m', m ~ t m')
                      => Int
                      -> m (Maybe e)
    loadEntry = lift . loadEntry

    default storeEntries :: (MonadTrans t, LogME e m', m ~ t m') => [e] -> m ()
    storeEntries = lift . storeEntries

type TID = Int

data LogEntry k v = LogEntry
    { _term  :: Int
    , _index :: Int
    , _data  :: LogEntryData k v
    } deriving (Show, Eq, Generic)

data LogEntryData k v = Insert TID k
                      | Change TID k v
                      | Transaction Transaction
                      | Checkpoint Checkpoint
                      deriving (Show, Eq, Generic)

data Transaction = Start TID
                 | Commit TID
                 | Abort TID
                 deriving (Show, Eq, Generic)

data Checkpoint = Begin [TID] | End deriving (Show, Eq, Generic)

instance (Binary k, Binary v) => Entry (LogEntry k v) where
    entryIndex = _index
    entryTerm = _term

instance (Binary k, Binary v) => Binary (LogEntryData k v)
instance Binary Transaction
instance Binary Checkpoint
instance (Binary k, Binary v) => Binary (LogEntry k v)

data Log e = Log
    { _entries :: IM.IntMap e
    , _highIdx :: Int
    , _lowIdx  :: Int
    }

newtype LogT e m a = LogT { unLogT :: ReaderT (TVar (Log e)) m a }
    deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (TVar (Log e))
        )

execLogT :: (MonadIO m) => LogT e m a -> Log e -> m a
execLogT (LogT (ReaderT f)) = f <=< liftIO . newTVarIO

execLogTVar :: LogT e m a -> TVar (Log e) -> m a
execLogTVar (LogT (ReaderT f)) = f

instance (MonadIO m) => LogM (LogT e m) where
    firstIndex = return . _lowIdx =<< liftIO . readTVarIO =<< ask
    lastIndex = return . _highIdx =<< liftIO . readTVarIO =<< ask
    deleteRange a b = liftIO . modifyTVarIO (deleteRangeLog a b) =<< ask

instance (MonadIO m, Entry e) => LogME e (LogT e m) where
    loadEntry k =
        return . IM.lookup k . _entries =<< liftIO . readTVarIO =<< ask
    storeEntries es = liftIO . modifyTVarIO (storeEntriesLog es) =<< ask

instance (StorageM m) => StorageM (LogT e m)
instance (StorageMK k m) => StorageMK k (LogT e m)
instance (StorageMKV k v m) => StorageMKV k v (LogT e m)
instance (LogM m) => LogM (ReaderT r m)
instance (LogME e m) => LogME e (ReaderT r m)

deleteRangeLog :: Int -> Int -> Log e -> Log e
deleteRangeLog min max l =
    l { _lowIdx = lowIndex', _highIdx = highIndex', _entries = entries' }
  where
    entries' = foldl' (flip IM.delete) (_entries l) [min..max]
    lowIndex = if min <= _lowIdx l then max + 1 else _lowIdx l
    highIndex = if max >= _highIdx l then min - 1 else _highIdx l
    (lowIndex', highIndex') = if lowIndex > highIndex
        then (0, 0)
        else (lowIndex, highIndex)

storeEntriesLog :: (Entry e) => [e] -> Log e -> Log e
storeEntriesLog es l = foldl' addEntry l es
  where
    addEntry l e =
        l { _entries = entries', _lowIdx = lowIndex, _highIdx = highIndex }
      where
        index = (entryIndex e)
        entries' = IM.insert index e (_entries l)
        lowIndex = if _lowIdx l == 0 then index else _lowIdx l
        highIndex = if index > _highIdx l then index else _highIdx l
