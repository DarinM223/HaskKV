{-# LANGUAGE TemplateHaskell #-}

module Mock where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft
import HaskKV.Store hiding (HasStore)

import qualified Data.IntMap as IM

type K = Int
type V = StoreValue Int
type E = LogEntry K V
type M = RaftMessage E

data MockConfig = MockConfig
    { _state          :: RaftState
    , _store          :: StoreData K V E
    , _tempLog        :: [E]
    , _messages       :: [M]
    , _electionTimer  :: Bool
    , _heartbeatTimer :: Bool
    , _appliedEntries :: [E]
    }
makeFieldsNoPrefix ''MockConfig

-- TODO(DarinM223): use state monad and "zoom" into each MockConfig.
-- This means that StateT MockConfig needs instances for ServerM, StorageM,
-- ApplyEntryM, LogM, LoadSnapshotM, TakeSnapshotM, TempLogM, and
-- SnapshotM.

type Servers = [MockConfig]

instance TempLogM E (State MockConfig) where
    addTemporaryEntry = undefined
    temporaryEntries = undefined

instance LogM E (State MockConfig) where
    firstIndex = _lowIdx . _log <$> gets _store
    lastIndex = lastIndexLog . _log <$> gets _store
    loadEntry k = IM.lookup k . getField @"_entries" . _log <$> gets _store
    termFromIndex i = entryTermLog i . _log <$> gets _store
    storeEntries es = store %= (\s -> s { _log = storeEntriesLog es (_log s) })
    deleteRange a b = store %= (\s -> s { _log = deleteRangeLog a b (_log s) })

instance StorageM K V (State MockConfig) where
    getValue = undefined
    setValue = undefined
    replaceValue = undefined
    deleteValue = undefined
    cleanupExpired = undefined
