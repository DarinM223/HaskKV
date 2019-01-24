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
     , HasDebugM effs (DebugM m)
     , HasLogM effs (LogM e m)
     , HasTempLogM effs (TempLogM e m)
     , HasServerM effs (ServerM (RaftMessage e) ServerEvent m)
     , HasSnapshotM effs (SnapshotM s m)
     , HasPersistM effs (PersistM m)
     , Entry e )
  => effs -> m ()
runLeader effs = recv >>= \case
  Left ElectionTimeout  -> reset ElectionTimeout
  Left HeartbeatTimeout -> do
    reset HeartbeatTimeout
    commitIndex' <- use commitIndex
    serverID'    <- use serverID

    storeTemporaryEntries effs

    -- Update the highest replicated index for our server.
    lastIndex' <- lastIndex
    stateType . _Leader . matchIndex %= IM.insert (unSID serverID') lastIndex'
    ids <- serverIds
    let otherServerIds = filter (/= serverID') ids
    debug "Sending AppendEntries"
    mapM_ (sendAppendEntries effs lastIndex' commitIndex') otherServerIds
  Right rv@RequestVote{}       -> get >>= handleRequestVote effs rv
  Right ae@AppendEntries{}     -> get >>= handleAppendEntries effs ae
  Right InstallSnapshot{}      -> return ()
  Right (Response sender resp) -> get >>= handleLeaderResponse effs sender resp
 where
  ServerM { recv, reset, serverIds } = effs ^. serverM
  LogM { lastIndex } = effs ^. logM
  DebugM debug = effs ^. debugM

handleLeaderResponse
  :: ( MonadState RaftState m
     , HasDebugM effs (DebugM m)
     , HasLogM effs (LogM e m)
     , HasServerM effs (ServerM (RaftMessage e) event m)
     , HasSnapshotM effs (SnapshotM s m)
     , HasPersistM effs (PersistM m) )
  => effs -> SID -> RaftResponse -> RaftState -> m ()
handleLeaderResponse
  effs (SID sender) msg@(AppendResponse term success lastIndex) s
  | term < getField @"_currTerm" s = return ()
  | term > getField @"_currTerm" s = transitionToFollower persistM' msg
  | not success = do
    debug $ "Decrementing next index for server " ++ show sender
    stateType . _Leader . nextIndex %= IM.adjust prevIndex sender
  | otherwise = do
    debug $ "Updating indexes for server " ++ show sender
    stateType . _Leader . matchIndex %= IM.adjust (max lastIndex) sender
    stateType . _Leader . nextIndex %= IM.adjust (max (lastIndex + 1)) sender

    -- If there exists an N such that N > commitIndex,
    -- a majority of matchIndex[i] >= N, and
    -- log[N].term = currentTerm, set commitIndex = N.
    n    <- quorumIndex serverM'
    term <- fromMaybe 0 <$> termFromIndex n
    debug $ "N: " ++ show n
    debug $ "Commit Index: " ++ show (_commitIndex s)
    when (n > _commitIndex s && term == getField @"_currTerm" s) $ do
      debug $ "Updating commit index to " ++ show n
      commitIndex .= n
 where
  persistM' = effs ^. persistM
  DebugM debug = effs ^. debugM
  serverM' = effs ^. serverM
  LogM { termFromIndex } = effs ^. logM
handleLeaderResponse effs sender msg@(InstallSnapshotResponse term) s
  | term < getField @"_currTerm" s = return ()
  | term > getField @"_currTerm" s = transitionToFollower persistM' msg
  | otherwise = do
    hasRemaining <- hasChunk sender
    if hasRemaining
      then do
        chunk <- readChunk snapshotChunkSize sender
        mapM_ (sendSnapshotChunk serverM' sender) chunk
      else snapshotInfo >>= \info -> forM_ info $ \(i, _, _) -> do
        let sid = unSID sender
        stateType . _Leader . matchIndex %= IM.adjust (max i) sid
        stateType . _Leader . nextIndex %= IM.adjust (max (i + 1)) sid
 where
  persistM' = effs ^. persistM
  SnapshotM { hasChunk, readChunk, snapshotInfo } = effs ^. snapshotM
  serverM' = effs ^. serverM
handleLeaderResponse _ _ _ _ = return ()

quorumIndex :: (MonadState RaftState m) => ServerM msg event m -> m LogIndex
quorumIndex serverM = do
  matchIndexes <- maybe [] IM.elems
              <$> preuse (stateType . _Leader . matchIndex)
  let sorted = sortBy (flip compare) matchIndexes
  quorumSize' <- quorumSize serverM
  return $ sorted !! (quorumSize' - 1)

storeTemporaryEntries :: ( MonadState RaftState m
                         , HasDebugM effs (DebugM m)
                         , HasLogM effs (LogM e m)
                         , HasTempLogM effs (TempLogM e m)
                         , Entry e )
                      => effs -> m ()
storeTemporaryEntries effs = do
  term       <- use currTerm
  lastIndex' <- lastIndex

  entries <- setIndexAndTerms (lastIndex' + 1) term <$> temporaryEntries
  debug $ "Storing temporary entries: " ++ show entries
  storeEntries entries
 where
  LogM { lastIndex, storeEntries } = effs ^. logM
  TempLogM { temporaryEntries } = effs ^. tempLogM
  DebugM debug = effs ^. debugM

  setIndexAndTerms _   _    []       = []
  setIndexAndTerms idx term (e : es) = e' : rest
   where
    e'   = setEntryTerm term . setEntryIndex idx $ e
    rest = setIndexAndTerms (idx + 1) term es

snapshotChunkSize :: Int
snapshotChunkSize = 10

sendAppendEntries
  :: ( MonadState RaftState m
     , HasLogM effs (LogM e m)
     , HasServerM effs (ServerM (RaftMessage e) event m)
     , HasSnapshotM effs (SnapshotM s m) )
  => effs -> LogIndex -> LogIndex -> SID -> m ()
sendAppendEntries effs lastIndex commitIndex id = do
  nextIndexes <- preuse (stateType . _Leader . nextIndex)
  case nextIndexes >>= IM.lookup (unSID id) of
    Just nextIndex -> do
      let pi = prevIndex nextIndex
      pt      <- termFromIndex $ prevIndex nextIndex
      entries <- entryRange logM' nextIndex lastIndex
      if isNothing entries || isNothing pt
        then tryInstallSnapshot snapM serverM' id pi pt commitIndex
        else sendAppend serverM' id pi pt (fromMaybe [] entries) commitIndex
    Nothing -> sendAppend serverM' id 0 Nothing [] commitIndex
 where
  logM'@LogM { termFromIndex } = effs ^. logM
  snapM = effs ^. snapshotM
  serverM' = effs ^. serverM

  sendAppend ServerM{ send } id prevIndex prevTerm entries commitIndex = do
    term <- use currTerm
    sid  <- use serverID
    send id AppendEntries
      { _term        = term
      , _leaderId    = sid
      , _prevLogIdx  = prevIndex
      , _prevLogTerm = fromMaybe 0 prevTerm
      , _entries     = entries
      , _commitIdx   = commitIndex
      }
  tryInstallSnapshot SnapshotM{ readChunk } serverM id pi pt commitIndex =
    readChunk snapshotChunkSize id >>= \case
      Just chunk -> sendSnapshotChunk serverM id chunk
      Nothing    -> sendAppend serverM id pi pt [] commitIndex

sendSnapshotChunk
  :: MonadState RaftState m
  => ServerM (RaftMessage e) event m -> SID -> SnapshotChunk -> m ()
sendSnapshotChunk ServerM{ send } id chunk = do
  term <- use currTerm
  sid  <- use serverID
  let done      = _type chunk == EndChunk
      lastIndex = getField @"_index" chunk
      lastTerm  = getField @"_term" chunk
      offset    = getField @"_offset" chunk
      snapData  = getField @"_data" chunk
  send id InstallSnapshot
    { _term              = term
    , _leaderId          = sid
    , _lastIncludedIndex = lastIndex
    , _lastIncludedTerm  = lastTerm
    , _offset            = offset
    , _data              = snapData
    , _done              = done
    }
