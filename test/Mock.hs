{-# LANGUAGE TemplateHaskell #-}

module Mock where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import Data.Monoid
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
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as CL
import qualified Data.IntMap as IM
import qualified Data.Map as M

type K = Int
type V = StoreValue Int
type E = LogEntry K V
type M = RaftMessage E

data MockSnapshot = MockSnapshot
    { _file   :: String
    , _sIndex :: Int
    , _term   :: Int
    }
makeFieldsNoPrefix ''MockSnapshot

data MockSnapshotManager = MockSnapshotManager
    { _completed :: Maybe MockSnapshot
    , _partial   :: IM.IntMap MockSnapshot
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
    , _files           :: M.Map (Int, Int) String
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

prezoom l m = getFirst <$> zoom l (First . Just <$> m)

instance SnapshotM (M.Map K V) (State MockConfig) where
    createSnapshot i t = do
        file <- preuse (files . ix (i, t))
        forM_ file $ \file -> do
            let snapshot = MockSnapshot { _file   = file
                                        , _sIndex = i
                                        , _term   = t
                                        }
            (snapshotManager . partial) %= (IM.insert i snapshot)
    writeSnapshot _ snapData i =
        (snapshotManager . partial . ix i . file) %= (++ (C.unpack snapData))
    saveSnapshot i = do
        snap <- fromJust <$> preuse (snapshotManager . partial . ix i)
        (snapshotManager . completed . _Just) %= \s ->
            if getField @"_sIndex" s < i then snap else s
        (snapshotManager . partial) %= IM.filter ((> i) . getField @"_sIndex")
    readSnapshot _ = do
        snapData <- preuse (snapshotManager . completed . _Just . file)
        return . fmap (decode . CL.pack) $ snapData
    readChunk amount sid = do
        Just (i, t) <- snapshotInfo
        temp <- preuse (snapshotManager . chunks . ix sid)
        when (isNothing temp || temp == Just "") $ do
            file <- use (files . ix (i, t))
            (snapshotManager . chunks) %= (IM.insert sid file)
        prezoom (snapshotManager . chunks . ix sid) $ S.state $ \s ->
            let (chunkData, remainder) = splitAt amount s
                chunkType = if null remainder then EndChunk else FullChunk
                chunk = SnapshotChunk { _data   = C.pack chunkData
                                      , _type   = chunkType
                                      , _offset = 0
                                      , _index  = i
                                      , _term   = t
                                      }
            in (chunk, remainder)
    snapshotInfo = do
        snapIndex <- preuse (snapshotManager . completed . _Just . sIndex)
        snapTerm <- preuse (snapshotManager . completed . _Just . term)
        return $ (,) <$> snapIndex <*> snapTerm
