{-# LANGUAGE TemplateHaskell #-}

module Mock where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import Data.Binary
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft
import HaskKV.Snapshot hiding (HasSnapshotManager)
import HaskKV.Store hiding (HasStore)

import qualified Control.Monad.State as S
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM
import qualified Data.Map as M

type K = Int
type V = StoreValue Int
type E = LogEntry K V
type M = RaftMessage E

data MockSnapshot = MockSnapshot
    { _file   :: String
    , _index  :: Int
    , _term   :: Int
    , _offset :: Int
    }
makeFieldsNoPrefix ''MockSnapshot

data MockSnapshotManager = MockSnapshotManager
    { _completed :: Maybe MockSnapshot
    , _partial   :: [MockSnapshot]
    , _chunks    :: IM.IntMap String
    }
makeFieldsNoPrefix ''MockSnapshotManager

data MockConfig = MockConfig
    { _state           :: RaftState
    , _store           :: StoreData K V E
    , _tempLog         :: [E]
    , _snapshotManager :: MockSnapshotManager
    , _messages        :: [M]
    , _electionTimer   :: Bool
    , _heartbeatTimer  :: Bool
    , _appliedEntries  :: [E]
    }
makeFieldsNoPrefix ''MockConfig

-- TODO(DarinM223): use state monad and "zoom" into each MockConfig.
-- This means that StateT MockConfig needs instances for ServerM, StorageM,
-- ApplyEntryM, LogM, LoadSnapshotM, TakeSnapshotM, TempLogM, and
-- SnapshotM.

type Servers = [MockConfig]

instance TempLogM E (State MockConfig) where
    addTemporaryEntry e = tempLog %= (++ [e])
    temporaryEntries = do
        log <- use tempLog
        tempLog .= []
        return log

instance LogM E (State MockConfig) where
    firstIndex = _lowIdx . _log <$> gets _store
    lastIndex = lastIndexLog . _log <$> gets _store
    loadEntry k = IM.lookup k . getField @"_entries" . _log <$> gets _store
    termFromIndex i = entryTermLog i . _log <$> gets _store
    storeEntries es = store %= (\s -> s { _log = storeEntriesLog es (_log s) })
    deleteRange a b = store %= (\s -> s { _log = deleteRangeLog a b (_log s) })

instance StorageM K V (State MockConfig) where
    getValue k = getKey k <$> gets _store
    setValue k v = store %= setKey k v
    replaceValue k v = zoom store $ S.state $ replaceKey k v
    deleteValue k = store %= deleteKey k
    cleanupExpired t = store %= cleanupStore t

instance LoadSnapshotM (M.Map K V) (State MockConfig) where
    loadSnapshot i t m = store %= loadSnapshotStore i t m

instance TakeSnapshotM (State MockConfig) where
    takeSnapshot = do
        storeData <- gets _store
        let firstIndex = _lowIdx $ _log storeData
            lastIndex  = _highIdx $ _log storeData
            lastTerm   = entryTerm . fromJust
                       . IM.lookup lastIndex
                       . getField @"_entries" . _log
                       $ storeData
        createSnapshot lastIndex lastTerm
        let snapData = B.concat . BL.toChunks . encode . _map $ storeData
        writeSnapshot 0 snapData lastIndex
        saveSnapshot lastIndex
        deleteRange firstIndex lastIndex

instance ApplyEntryM K V E (State MockConfig) where
    applyEntry entry@LogEntry{ _data = entryData } = do
        appliedEntries %= (++ [entry])
        case entryData of
            Change _ k v -> setValue k v
            Delete _ k   -> deleteValue k
            _            -> return ()

{-createSnapshot :: Int -> Int -> m ()-}
{-writeSnapshot  :: Int -> B.ByteString -> Int -> m ()-}
{-saveSnapshot   :: Int -> m ()-}
{-readSnapshot   :: Int -> m (Maybe s)-}
{-readChunk      :: Int -> Int -> m (Maybe SnapshotChunk)-}
{-snapshotInfo   :: m (Maybe (Int, Int))-}
instance SnapshotM (M.Map K V) (State MockConfig) where
    createSnapshot i t = undefined
    writeSnapshot offset snapData i = undefined
    saveSnapshot i = undefined
    readSnapshot i = undefined
    readChunk i t = undefined
    snapshotInfo = undefined
