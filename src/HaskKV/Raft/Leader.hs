{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeFamilies #-}
module HaskKV.Raft.Leader where

import Control.Monad.State (MonadState (get), when)
import Data.Foldable (for_, traverse_)
import Data.List (sortBy)
import Data.Maybe (fromMaybe, isNothing)
import HaskKV.Log.Class
import HaskKV.Log.Utils (entryRange, prevIndex)
import HaskKV.Raft.Class (PersistM, DebugM (..))
import HaskKV.Raft.Message
import HaskKV.Raft.RPC (handleAppendEntries, handleRequestVote)
import HaskKV.Raft.State (_Leader, RaftState)
import HaskKV.Raft.Utils (quorumSize, transitionToFollower)
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Types (LogIndex, SID (..))
import Optics ((%), (^.), preuse, guse)
import Optics.State.Operators ((%=), (.=))

import qualified Data.IntMap as IM

runLeader
  :: ( DebugM m
     , MonadState RaftState m
     , LogM e m
     , TempLogM e m
     , ServerM (RaftMessage e) ServerEvent m
     , SnapshotM s m
     , PersistM m
     , Entry e
     )
  => m ()
runLeader = recv >>= \case
  Left ElectionTimeout  -> reset ElectionTimeout
  Left HeartbeatTimeout -> do
    reset HeartbeatTimeout
    commitIndex' <- guse #commitIndex
    serverID'    <- guse #serverID

    storeTemporaryEntries

    -- Update the highest replicated index for our server.
    lastIndex' <- lastIndex
    #stateType % _Leader % #matchIndex %= IM.insert (unSID serverID') lastIndex'
    ids <- serverIds
    let otherServerIds = filter (/= serverID') ids
    debug "Sending AppendEntries"
    traverse_ (sendAppendEntries lastIndex' commitIndex') otherServerIds
  Right (RequestVote rv)       -> get >>= handleRequestVote rv
  Right (AppendEntries ae)     -> get >>= handleAppendEntries ae
  Right (InstallSnapshot _)    -> return ()
  Right (Response sender resp) -> get >>= handleLeaderResponse sender resp

handleLeaderResponse
  :: ( DebugM m
     , MonadState RaftState m
     , LogM e m
     , ServerM (RaftMessage e) event m
     , SnapshotM s m
     , PersistM m
     )
  => SID
  -> RaftResponse
  -> RaftState
  -> m ()
handleLeaderResponse (SID sender) msg@(AppendResponse term success lastIndex) s
  | term < s ^. #currTerm = return ()
  | term > s ^. #currTerm = transitionToFollower msg
  | not success = do
    debug $ "Decrementing next index for server " ++ show sender
    #stateType % _Leader % #nextIndex %= IM.adjust prevIndex sender
  | otherwise = do
    debug $ "Updating indexes for server " ++ show sender
    #stateType % _Leader % #matchIndex %= IM.adjust (max lastIndex) sender
    #stateType % _Leader % #nextIndex %= IM.adjust (max (lastIndex + 1)) sender

    -- If there exists an N such that N > commitIndex,
    -- a majority of matchIndex[i] >= N, and
    -- log[N].term = currentTerm, set commitIndex = N.
    n    <- quorumIndex
    term <- fromMaybe 0 <$> termFromIndex n
    debug $ "N: " ++ show n
    debug $ "Commit Index: " ++ show (s ^. #commitIndex)
    when (n > s ^. #commitIndex && term == s ^. #currTerm) $ do
      debug $ "Updating commit index to " ++ show n
      #commitIndex .= n
handleLeaderResponse sender msg@(InstallSnapshotResponse term) s
  | term < s ^. #currTerm = return ()
  | term > s ^. #currTerm = transitionToFollower msg
  | otherwise = do
    hasRemaining <- hasChunk sender
    if hasRemaining
      then do
        chunk <- readChunk snapshotChunkSize sender
        traverse_ (sendSnapshotChunk sender) chunk
      else snapshotInfo >>= \info -> for_ info $ \(i, _, _) -> do
        let sid = unSID sender
        #stateType % _Leader % #matchIndex %= IM.adjust (max i) sid
        #stateType % _Leader % #nextIndex %= IM.adjust (max (i + 1)) sid
handleLeaderResponse _ _ _ = return ()

quorumIndex :: (MonadState RaftState m, ServerM msg event m) => m LogIndex
quorumIndex = do
  matchIndexes <- maybe [] IM.elems
              <$> preuse (#stateType % _Leader % #matchIndex)
  let sorted = sortBy (flip compare) matchIndexes
  quorumSize' <- quorumSize
  return $ sorted !! (quorumSize' - 1)

storeTemporaryEntries
  :: (DebugM m, MonadState RaftState m, LogM e m, TempLogM e m, Entry e) => m ()
storeTemporaryEntries = do
  term       <- guse #currTerm
  lastIndex' <- lastIndex

  entries    <- setIndexAndTerms (lastIndex' + 1) term <$> temporaryEntries
  debug $ "Storing temporary entries: " ++ show entries
  storeEntries entries
 where
  setIndexAndTerms _   _    []       = []
  setIndexAndTerms idx term (e : es) = e' : rest
   where
    e'   = setEntryTerm term . setEntryIndex idx $ e
    rest = setIndexAndTerms (idx + 1) term es

snapshotChunkSize :: Int
snapshotChunkSize = 10

sendAppendEntries
  :: forall event e s m
   . ( MonadState RaftState m
     , LogM e m
     , ServerM (RaftMessage e) event m
     , SnapshotM s m
     )
  => LogIndex
  -> LogIndex
  -> SID
  -> m ()
sendAppendEntries lastIndex commitIndex id = do
  nextIndexes <- preuse (#stateType % _Leader % #nextIndex)
  case nextIndexes >>= IM.lookup (unSID id) of
    Just nextIndex -> do
      let pi = prevIndex nextIndex
      pt      <- termFromIndex $ prevIndex nextIndex
      entries <- entryRange nextIndex lastIndex
      if isNothing entries || isNothing pt
        then tryInstallSnapshot id pi pt commitIndex
        else sendAppend id pi pt (fromMaybe [] entries) commitIndex
    Nothing -> sendAppend id 0 Nothing [] commitIndex
 where
  sendAppend id prevIndex prevTerm entries commitIndex = do
    term <- guse #currTerm
    sid  <- guse #serverID
    send id $ AppendEntries $ AppendEntries'
      { aeTerm        = term
      , aeLeaderId    = sid
      , aePrevLogIdx  = prevIndex
      , aePrevLogTerm = fromMaybe 0 prevTerm
      , aeEntries     = entries
      , aeCommitIdx   = commitIndex
      }
  tryInstallSnapshot id pi pt commitIndex =
    readChunk snapshotChunkSize id >>= \case
      Just chunk -> sendSnapshotChunk id chunk
      Nothing    -> sendAppend id pi pt [] commitIndex

sendSnapshotChunk
  :: (MonadState RaftState m, ServerM (RaftMessage e) event m)
  => SID
  -> SnapshotChunk
  -> m ()
sendSnapshotChunk id chunk = do
  term <- guse #currTerm
  sid  <- guse #serverID
  send id $ InstallSnapshot $ InstallSnapshot'
    { isTerm              = term
    , isLeaderId          = sid
    , isLastIncludedIndex = chunk ^. #index
    , isLastIncludedTerm  = chunk ^. #term
    , isOffset            = chunk ^. #offset
    , isData              = chunk ^. #chunkData
    , isDone              = chunk ^. #chunkType == EndChunk
    }
