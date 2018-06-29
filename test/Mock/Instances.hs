{-# LANGUAGE TemplateHaskell #-}

module Mock.Instances where

import Control.Lens
import Control.Monad.State.Strict
import Data.Maybe
import Data.Monoid
import Data.Binary
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft
import HaskKV.Raft.Debug
import HaskKV.Server
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
    } deriving (Show)
makeFieldsNoPrefix ''MockSnapshot

data MockSnapshotManager = MockSnapshotManager
    { _completed :: Maybe MockSnapshot
    , _partial   :: IM.IntMap MockSnapshot
    , _chunks    :: IM.IntMap (String, Int)
    } deriving (Show)
makeFieldsNoPrefix ''MockSnapshotManager

data MockConfig = MockConfig
    { _raftState       :: RaftState
    , _store           :: StoreData K V E
    , _tempLog         :: [E]
    , _snapshotManager :: MockSnapshotManager
    , _receivingMsgs   :: [M]
    , _sendingMsgs     :: [(Int, M)]
    , _myServerID      :: Int
    , _serverIDs       :: [Int]
    , _electionTimer   :: Bool
    , _heartbeatTimer  :: Bool
    , _appliedEntries  :: [E]
    } deriving (Show)
makeFieldsNoPrefix ''MockConfig

newMockSnapshotManager :: MockSnapshotManager
newMockSnapshotManager = MockSnapshotManager
    { _completed = Nothing
    , _partial   = IM.empty
    , _chunks    = IM.empty
    }

newMockConfig :: [Int] -> Int -> MockConfig
newMockConfig sids sid = MockConfig
    { _raftState       = newRaftState sid
    , _store           = emptyStoreData
    , _tempLog         = []
    , _snapshotManager = newMockSnapshotManager
    , _receivingMsgs   = []
    , _sendingMsgs     = []
    , _myServerID      = sid
    , _serverIDs       = sids
    , _electionTimer   = False
    , _heartbeatTimer  = False
    , _appliedEntries  = []
    }

newtype MockT s a = MockT { unMockT :: State s a }
    deriving (Functor, Applicative, Monad)
deriving instance TempLogM E (MockT MockConfig)
deriving instance TakeSnapshotM (MockT MockConfig)
deriving instance LogM E (MockT MockConfig)
deriving instance StorageM K V (MockT MockConfig)
deriving instance LoadSnapshotM (M.Map K V) (MockT MockConfig)
deriving instance ApplyEntryM K V E (MockT MockConfig)
deriving instance SnapshotM (M.Map K V) (MockT MockConfig)
deriving instance ServerM M ServerEvent (MockT MockConfig)

runMockT :: MockT s a -> s -> (a, s)
runMockT m s = flip runState s . unMockT $ m

instance MonadState RaftState (MockT MockConfig) where
    get = MockT (gets _raftState)
    put v = MockT (raftState .= v)

instance DebugM (MockT MockConfig) where
    debug _ = return ()

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
        lastIndex <- use (raftState . lastApplied)
        storeData <- gets _store
        let firstIndex = _lowIdx $ _log storeData
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
        let snapshot = MockSnapshot { _file = "", _sIndex = i, _term = t }
        (snapshotManager . partial) %= (IM.insert i snapshot)
    writeSnapshot _ snapData i =
        (snapshotManager . partial . ix i . file) %= (++ (C.unpack snapData))
    saveSnapshot i = do
        snap <- fromJust <$> preuse (snapshotManager . partial . ix i)
        (snapshotManager . completed) %= \case
            Just s | getField @"_sIndex" s < i -> Just snap
            Nothing                            -> Just snap
            s                                  -> s
        (snapshotManager . partial) %= IM.filter ((> i) . getField @"_sIndex")
    readSnapshot _ = do
        snapData <- preuse (snapshotManager . completed . _Just . file)
        return . fmap (decode . CL.pack) $ snapData
    hasChunk sid =
        preuse (snapshotManager . chunks . ix sid) >>= pure . \case
            Just (s, _) | s == "" -> False
            Nothing               -> False
            _                     -> True
    readChunk amount sid = snapshotInfo >>= \case
        Just (i, t) -> do
            temp <- preuse (snapshotManager . chunks . ix sid)
            when (isNothing temp || fmap fst temp == Just "") $ do
                file' <- use (snapshotManager . completed . _Just . file)
                (snapshotManager . chunks) %= (IM.insert sid (file', 0))
            prezoom (snapshotManager . chunks . ix sid) $ S.state $ \(s, f) ->
                let (chunkData, remainder) = splitAt amount s
                    chunkType = if null remainder then EndChunk else FullChunk
                    chunk = SnapshotChunk { _data   = C.pack chunkData
                                          , _type   = chunkType
                                          , _offset = f
                                          , _index  = i
                                          , _term   = t
                                          }
                in (chunk, (remainder, f + 1))
        _ -> return Nothing
    snapshotInfo = do
        snapIndex <- preuse (snapshotManager . completed . _Just . sIndex)
        snapTerm <- preuse (snapshotManager . completed . _Just . term)
        return $ (,) <$> snapIndex <*> snapTerm

instance ServerM M ServerEvent (State MockConfig) where
    send sid msg = sendingMsgs %= (++ [(sid, msg)])
    broadcast msg = do
        sid <- use myServerID
        sids <- use serverIDs
        mapM_ (flip send msg) . filter (/= sid) $ sids
    recv = S.get >>= go
      where
        go s
            | getField @"_electionTimer" s = do
                electionTimer .= False
                return (Left ElectionTimeout)
            | getField @"_heartbeatTimer" s = do
                heartbeatTimer .= False
                return (Left HeartbeatTimeout)
            | otherwise = do
                msgs <- use receivingMsgs
                let (msg, msgs') = splitAt 1 msgs
                receivingMsgs .= msgs'
                return $ Right $ head msg
    reset HeartbeatTimeout = heartbeatTimer .= False
    reset ElectionTimeout = electionTimer .= False
    serverIds = gets _serverIDs
