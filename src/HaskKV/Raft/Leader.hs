module HaskKV.Raft.Leader where

import Control.Lens
import Control.Monad.State
import Data.List (sortBy)
import Data.Maybe
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Utils
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Types

import qualified Data.IntMap as IM

runLeader
  :: ( MonadState RaftState m
     , HasDebugM m effs
     , HasLogM e m effs
     , HasTempLogM e m effs
     , HasServerM (RaftMessage e) ServerEvent m effs
     , HasSnapshotM s m effs
     , HasPersistM m effs
     , Entry e )
  => effs -> m ()
runLeader effs = recv serverM >>= \case
  Left ElectionTimeout  -> reset serverM ElectionTimeout
  Left HeartbeatTimeout -> do
    reset serverM HeartbeatTimeout
    commitIndex' <- use commitIndex
    serverID'    <- use serverID

    storeTemporaryEntries effs

    -- Update the highest replicated index for our server.
    lastIndex' <- lastIndex logM
    stateType . _Leader . matchIndex %= IM.insert (unSID serverID') lastIndex'
    ids <- serverIds serverM
    let otherServerIds = filter (/= serverID') ids
    debug debugM "Sending AppendEntries"
    mapM_ (sendAppendEntries effs lastIndex' commitIndex') otherServerIds
  Right rv@RequestVote{}       -> get >>= handleRequestVote effs rv
  Right ae@AppendEntries{}     -> get >>= handleAppendEntries effs ae
  Right InstallSnapshot{}      -> return ()
  Right (Response sender resp) -> get >>= handleLeaderResponse effs sender resp
 where
  serverM = getServerM effs
  logM = getLogM effs
  debugM = getDebugM effs

handleLeaderResponse
  :: ( MonadState RaftState m
     , HasDebugM m effs
     , HasLogM e m effs
     , HasServerM (RaftMessage e) event m effs
     , HasSnapshotM s m effs
     , HasPersistM m effs )
  => effs -> SID -> RaftResponse -> RaftState -> m ()
handleLeaderResponse
  effs (SID sender) msg@(AppendResponse term success lastIndex) s
  | term < getField @"_currTerm" s = return ()
  | term > getField @"_currTerm" s = transitionToFollower persistM msg
  | not success = do
    debug debugM $ "Decrementing next index for server " ++ show sender
    stateType . _Leader . nextIndex %= IM.adjust prevIndex sender
  | otherwise = do
    debug debugM $ "Updating indexes for server " ++ show sender
    stateType . _Leader . matchIndex %= IM.adjust (max lastIndex) sender
    stateType . _Leader . nextIndex %= IM.adjust (max (lastIndex + 1)) sender

    -- If there exists an N such that N > commitIndex,
    -- a majority of matchIndex[i] >= N, and
    -- log[N].term = currentTerm, set commitIndex = N.
    n    <- quorumIndex serverM
    term <- fromMaybe 0 <$> termFromIndex logM n
    debug debugM $ "N: " ++ show n
    debug debugM $ "Commit Index: " ++ show (_commitIndex s)
    when (n > _commitIndex s && term == getField @"_currTerm" s) $ do
      debug debugM $ "Updating commit index to " ++ show n
      commitIndex .= n
 where
  persistM = getPersistM effs
  debugM = getDebugM effs
  serverM = getServerM effs
  logM = getLogM effs
handleLeaderResponse effs sender msg@(InstallSnapshotResponse term) s
  | term < getField @"_currTerm" s = return ()
  | term > getField @"_currTerm" s = transitionToFollower persistM msg
  | otherwise = do
    hasRemaining <- hasChunk snapM sender
    if hasRemaining
      then do
        chunk <- readChunk snapM snapshotChunkSize sender
        mapM_ (sendSnapshotChunk serverM sender) chunk
      else snapshotInfo snapM >>= \info -> forM_ info $ \(i, _, _) -> do
        let sid = unSID sender
        stateType . _Leader . matchIndex %= IM.adjust (max i) sid
        stateType . _Leader . nextIndex %= IM.adjust (max (i + 1)) sid
 where
  persistM = getPersistM effs
  snapM = getSnapshotM effs
  serverM = getServerM effs
handleLeaderResponse _ _ _ _ = return ()

quorumIndex :: (MonadState RaftState m) => ServerM msg event m -> m LogIndex
quorumIndex serverM = do
  matchIndexes <- maybe [] IM.elems
              <$> preuse (stateType . _Leader . matchIndex)
  let sorted = sortBy (flip compare) matchIndexes
  quorumSize' <- quorumSize serverM
  return $ sorted !! (quorumSize' - 1)

storeTemporaryEntries :: ( MonadState RaftState m
                         , HasDebugM m effs
                         , HasLogM e m effs
                         , HasTempLogM e m effs
                         , Entry e )
                      => effs -> m ()
storeTemporaryEntries effs = do
  term       <- use currTerm
  lastIndex' <- lastIndex logM

  entries <- setIndexAndTerms (lastIndex' + 1) term
         <$> temporaryEntries tempLogM
  debug debugM $ "Storing temporary entries: " ++ show entries
  storeEntries logM entries
 where
  logM = getLogM effs
  tempLogM = getTempLogM effs
  debugM = getDebugM effs

  setIndexAndTerms _   _    []       = []
  setIndexAndTerms idx term (e : es) = e' : rest
   where
    e'   = setEntryTerm term . setEntryIndex idx $ e
    rest = setIndexAndTerms (idx + 1) term es

snapshotChunkSize :: Int
snapshotChunkSize = 10

sendAppendEntries
  :: ( MonadState RaftState m
     , HasLogM e m effs
     , HasServerM (RaftMessage e) event m effs
     , HasSnapshotM s m effs
     )
  => effs -> LogIndex -> LogIndex -> SID -> m ()
sendAppendEntries effs lastIndex commitIndex id = do
  nextIndexes <- preuse (stateType . _Leader . nextIndex)
  case nextIndexes >>= IM.lookup (unSID id) of
    Just nextIndex -> do
      let pi = prevIndex nextIndex
      pt      <- termFromIndex logM $ prevIndex nextIndex
      entries <- entryRange logM nextIndex lastIndex
      if isNothing entries || isNothing pt
        then tryInstallSnapshot snapM serverM id pi pt commitIndex
        else sendAppend serverM id pi pt (fromMaybe [] entries) commitIndex
    Nothing -> sendAppend serverM id 0 Nothing [] commitIndex
 where
  logM = getLogM effs
  snapM = getSnapshotM effs
  serverM = getServerM effs

  sendAppend serverM id prevIndex prevTerm entries commitIndex = do
    term <- use currTerm
    sid  <- use serverID
    send serverM id AppendEntries
      { _term        = term
      , _leaderId    = sid
      , _prevLogIdx  = prevIndex
      , _prevLogTerm = fromMaybe 0 prevTerm
      , _entries     = entries
      , _commitIdx   = commitIndex
      }
  tryInstallSnapshot snapM serverM id pi pt commitIndex =
    readChunk snapM snapshotChunkSize id >>= \case
      Just chunk -> sendSnapshotChunk serverM id chunk
      Nothing    -> sendAppend serverM id pi pt [] commitIndex

sendSnapshotChunk
  :: MonadState RaftState m
  => ServerM (RaftMessage e) event m -> SID -> SnapshotChunk -> m ()
sendSnapshotChunk serverM id chunk = do
  term <- use currTerm
  sid  <- use serverID
  let done      = _type chunk == EndChunk
      lastIndex = getField @"_index" chunk
      lastTerm  = getField @"_term" chunk
      offset    = getField @"_offset" chunk
      snapData  = getField @"_data" chunk
  send serverM id InstallSnapshot
    { _term              = term
    , _leaderId          = sid
    , _lastIncludedIndex = lastIndex
    , _lastIncludedTerm  = lastTerm
    , _offset            = offset
    , _data              = snapData
    , _done              = done
    }
