module HaskKV.Log where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Serialize (Serializable)
import HaskKV.Store
import HaskKV.Utils

import qualified Data.IntMap as IM

class (Serializable l) => Entry l where
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

data LogEntry = LogEntry
    { _term :: Int
    , _data :: LogEntryData
    } deriving (Show, Eq)

data LogEntryData
    = Insert
    | Change
    deriving (Show, Eq)

instance Entry LogEntry where
    -- TODO(DarinM223): implement this

instance Serializable LogEntry where
    -- TODO(DarinM223): implement this

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
deleteRangeLog = undefined

storeEntriesLog :: [e] -> Log e -> Log e
storeEntriesLog = undefined
