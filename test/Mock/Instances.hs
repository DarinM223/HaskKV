{-# LANGUAGE NoMonomorphismRestriction #-}
module Mock.Instances where

import Control.Monad.State.Strict
import Data.Binary
import Data.Maybe
import GHC.Generics
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Snapshot.Types hiding (HasSnapshotManager)
import HaskKV.Store.Types hiding (HasStore)
import HaskKV.Store.Utils
import HaskKV.Types
import Optics
import Optics.State.Operators

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
  { file   :: String
  , sIndex :: LogIndex
  , term   :: LogTerm
  } deriving (Show, Generic)

data MockSnapshotManager = MockSnapshotManager
  { completed :: Maybe MockSnapshot
  , partial   :: IM.IntMap MockSnapshot
  , chunks    :: IM.IntMap (String, FilePos)
  } deriving (Show, Generic)

data MockConfig = MockConfig
  { raftState       :: RaftState
  , store           :: StoreData K V E
  , tempLog         :: [E]
  , snapshotManager :: MockSnapshotManager
  , receivingMsgs   :: [M]
  , sendingMsgs     :: [(SID, M)]
  , myServerID      :: SID
  , serverIDs       :: [SID]
  , electionTimer   :: Bool
  , heartbeatTimer  :: Bool
  , appliedEntries  :: [E]
  } deriving (Show, Generic)

newMockSnapshotManager :: MockSnapshotManager
newMockSnapshotManager = MockSnapshotManager
  { completed = Nothing
  , partial   = IM.empty
  , chunks    = IM.empty
  }

newMockConfig :: [SID] -> SID -> MockConfig
newMockConfig sids sid = MockConfig
  { raftState       = newRaftState sid Nothing
  , store           = newStoreData sid Nothing
  , tempLog         = []
  , snapshotManager = newMockSnapshotManager
  , receivingMsgs   = []
  , sendingMsgs     = []
  , myServerID      = sid
  , serverIDs       = sids
  , electionTimer   = False
  , heartbeatTimer  = False
  , appliedEntries  = []
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
  get = MockT (use #raftState)
  put v = MockT (#raftState .= v)

instance DebugM (MockT MockConfig) where
  debug _ = return ()

instance PersistM (MockT MockConfig) where
  persist _ = return ()

instance TempLogM E (State MockConfig) where
  addTemporaryEntry e = #tempLog %= (++ [e])
  temporaryEntries = temporaryEntriesImpl

temporaryEntriesImpl = do
  log <- use #tempLog
  #tempLog .= []
  return log

instance LogM E (State MockConfig) where
  firstIndex = (^. #log % #lowIdx) <$> use #store
  lastIndex = lastIndexLog . (^. #log) <$> use #store
  loadEntry = loadEntryImpl
  termFromIndex i = entryTermLog i . (^. #log) <$> use #store
  storeEntries es = #store %= (#log %~ storeEntriesLog es)
  deleteRange a b = #store %= (#log %~ deleteRangeLog a b)

loadEntryImpl (LogIndex k) =
  IM.lookup k . (^. #log % #entries) <$> use #store

instance StorageM K V (State MockConfig) where
  getValue k = getKey k <$> use #store
  setValue k v = #store %= setKey k v
  replaceValue k v = zoom #store $ S.state $ replaceKey k v
  deleteValue k = #store %= deleteKey k
  cleanupExpired t = #store %= cleanupStore t

instance LoadSnapshotM (M.Map K V) (State MockConfig) where
  loadSnapshot i t m = #store %= loadSnapshotStore i t m

instance TakeSnapshotM (State MockConfig) where
  takeSnapshot = takeSnapshotImpl

takeSnapshotImpl = do
  lastIndex <- use $ #raftState % #lastApplied
  storeData <- use #store
  let
    firstIndex = storeData ^. #log % #lowIdx
    lastTerm =
      entryTerm
        . fromJust
        . IM.lookup (unLogIndex lastIndex)
        $ storeData ^. #log % #entries
  createSnapshot lastIndex lastTerm
  let snapData = B.concat . BL.toChunks . encode $ storeData ^. #map
  writeSnapshot 0 snapData lastIndex
  saveSnapshot lastIndex
  deleteRange firstIndex lastIndex
  snap <- readSnapshot lastIndex
  mapM_ (loadSnapshot lastIndex lastTerm) snap

instance ApplyEntryM K V E (State MockConfig) where
  applyEntry = applyEntryImpl

applyEntryImpl entry@LogEntry { entryData = entryData } = do
  #appliedEntries %= (++ [entry])
  case entryData of
    Change _ k v -> setValue k v
    Delete _ k   -> deleteValue k
    _            -> return ()

instance SnapshotM (M.Map K V) (State MockConfig) where
  createSnapshot = createSnapshotImpl
  writeSnapshot = writeSnapshotImpl
  saveSnapshot = saveSnapshotImpl
  readSnapshot = readSnapshotImpl
  hasChunk = hasChunkImpl
  readChunk = readChunkImpl
  snapshotInfo = snapshotInfoImpl

createSnapshotImpl i t = do
  let snapshot = MockSnapshot {file = "", sIndex = i, term = t}
  #snapshotManager % #partial %= IM.insert (unLogIndex i) snapshot
writeSnapshotImpl _ snapData (LogIndex i) =
  #snapshotManager % #partial % ix i % #file %= (++ (C.unpack snapData))
saveSnapshotImpl i = do
  let i' = unLogIndex i
  snap <- fromJust <$> preuse (#snapshotManager % #partial % ix i')
  #snapshotManager % #completed %= \case
    Just s | s ^. #sIndex < i -> Just snap
    Nothing                   -> Just snap
    s                         -> s
  #snapshotManager % #partial %= IM.filter ((> i) . (^. #sIndex))
readSnapshotImpl _ = do
  snapData <- preuse $ #snapshotManager % #completed % _Just % #file
  return . fmap (decode . CL.pack) $ snapData
hasChunkImpl (SID sid) =
  preuse (#snapshotManager % #chunks % ix sid) >>= pure . \case
    Just (s, _) | s == "" -> False
    Nothing               -> False
    _                     -> True
readChunkImpl amount (SID sid) = snapshotInfo >>= \case
  Just (i, t, _) -> do
    temp <- preuse $ #snapshotManager % #chunks % ix sid
    when (isNothing temp || fmap fst temp == Just "") $ do
      file' <- guse $ #snapshotManager % #completed % _Just % #file
      #snapshotManager % #chunks %= IM.insert sid (fromJust file', 0)
    zoomMaybe (#snapshotManager % #chunks % ix sid) $ S.state $ \(s, f) ->
      let
        (chunkData, remainder) = splitAt amount s
        chunkType              = if null remainder then EndChunk else FullChunk
        chunk                  = SnapshotChunk
          { chunkData = C.pack chunkData
          , chunkType = chunkType
          , offset    = f
          , index     = i
          , term      = t
          }
      in (chunk, (remainder, f + 1))
  _ -> return Nothing
snapshotInfoImpl = do
  snapIndex <- preuse $ #snapshotManager % #completed % _Just % #sIndex
  snapTerm  <- preuse $ #snapshotManager % #completed % _Just % #term
  file      <- preuse $ #snapshotManager % #completed % _Just % #file
  let snapSize = fmap (fromIntegral . length) file
  return $ (,,) <$> snapIndex <*> snapTerm <*> snapSize

instance ServerM M ServerEvent (State MockConfig) where
  send sid msg = #sendingMsgs %= (++ [(sid, msg)])
  broadcast = broadcastImpl
  recv = recvImpl
  reset HeartbeatTimeout = #heartbeatTimer .= False
  reset ElectionTimeout = #electionTimer .= False
  serverIds = use #serverIDs

broadcastImpl msg = do
  sid  <- use #myServerID
  sids <- use #serverIDs
  mapM_ (flip send msg) . filter (/= sid) $ sids
recvImpl = S.get >>= go
 where
  go s
    | s ^. #electionTimer = do
      #electionTimer .= False
      return (Left ElectionTimeout)
    | s ^. #heartbeatTimer = do
      #heartbeatTimer .= False
      return (Left HeartbeatTimeout)
    | otherwise = do
      msgs <- use #receivingMsgs
      let (msg, msgs') = splitAt 1 msgs
      #receivingMsgs .= msgs'
      return $ Right $ head msg
