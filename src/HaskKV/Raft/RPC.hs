{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Raft.RPC where

import Control.Monad.State (MonadState (get), when)
import Data.Foldable (traverse_)
import Data.Maybe (fromMaybe, isJust, isNothing)
import HaskKV.Log.Class (Entry (entryIndex, entryTerm), LogM (..))
import HaskKV.Log.Utils (diffEntriesWithLog)
import HaskKV.Raft.Class (PersistM (..), DebugM (..))
import HaskKV.Raft.Message
import HaskKV.Raft.State (RaftState)
import HaskKV.Raft.Utils (transitionToFollower)
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types (LoadSnapshotM (..), StorageM)
import Optics ((^.), guse)
import Optics.State.Operators ((.=))

handleRequestVote
  :: ( DebugM m
     , ServerM (RaftMessage e) ServerEvent m
     , MonadState RaftState m
     , LogM e m
     , PersistM m
     , Entry e
     )
  => RequestVote'
  -> RaftState
  -> m ()
handleRequestVote rv s
  | existingLeader rv s          = fail rv s
  | rv ^. #term < s ^. #currTerm = fail rv s
  | rv ^. #term > s ^. #currTerm = do
    debug "Transitioning to follower"
    transitionToFollower rv
    get >>= handleRequestVote rv
  | canVote (rv ^. #candidateID) s = do
    index <- lastIndex
    term  <- termFromIndex index
    let isValid = checkValid rv index (fromMaybe 0 term)
    if isValid
      then do
        debug $ "Sending vote to " ++ show (rv ^. #candidateID)
        #votedFor .= Just (rv ^. #candidateID)
        get >>= persist
        send (rv ^. #candidateID) $ successResponse s
        reset ElectionTimeout
      else fail rv s
  | otherwise = fail rv s
 where
  successResponse s =
    Response (s ^. #serverID) $ VoteResponse (s ^. #currTerm) True
  failResponse s =
    Response (s ^. #serverID) $ VoteResponse (s ^. #currTerm) False

  existingLeader rv s =
    isJust (s ^. #leader) && s ^. #leader /= Just (rv ^. #candidateID)
  canVote cid s = isNothing (s ^. #votedFor) || s ^. #votedFor == Just cid
  checkValid rv i t = rv ^. #lastLogIdx >= i && rv ^. #lastLogTerm >= t
  fail rv = send (rv ^. #candidateID) . failResponse
{-# INLINABLE handleRequestVote #-}

handleAppendEntries
  :: ( DebugM m
     , ServerM (RaftMessage e) ServerEvent m
     , MonadState RaftState m
     , LogM e m
     , PersistM m
     , Entry e
     )
  => AppendEntries' e
  -> RaftState
  -> m ()
handleAppendEntries ae s
  | ae ^. #term < s ^. #currTerm =
    send (ae ^. #leaderId) $ failResponse s
  | ae ^. #term > s ^. #currTerm = do
    debug "Transitioning to follower"
    transitionToFollower ae
    get >>= handleAppendEntries ae
  | otherwise = do
    #leader .= Just (ae ^. #leaderId)
    reset ElectionTimeout

    prevLogTerm <- termFromIndex $ ae ^. #prevLogIdx
    if prevLogTerm == Just (ae ^. #prevLogTerm)
      then do
        lastLogIndex <- lastIndex
        newEntries <- diffEntriesWithLog lastLogIndex $ ae ^. #entries
        storeEntries newEntries

        let lastEntryIndex = if null newEntries
              then lastLogIndex
              else entryIndex $ last newEntries

        when (lastEntryIndex /= lastLogIndex) $
          debug $ "Storing entries to index " ++ show lastEntryIndex

        commitIndex' <- guse #commitIndex
        when (ae ^. #commitIdx > commitIndex') $
          #commitIndex .= min lastEntryIndex (ae ^. #commitIdx)

        send (ae ^. #leaderId) $ successResponse lastEntryIndex s
      else send (ae ^. #leaderId) $ failResponse s
 where
  successResponse lastIndex s = Response (s ^. #serverID) $
    AppendResponse (s ^. #currTerm) True lastIndex
  failResponse s =
    Response (s ^. #serverID) $ AppendResponse (s ^. #currTerm) False 0
{-# INLINABLE handleAppendEntries #-}

handleInstallSnapshot
  :: ( StorageM k v m
     , LogM e m
     , ServerM (RaftMessage e) ServerEvent m
     , SnapshotM s m
     , LoadSnapshotM s m
     , Entry e
     )
  => InstallSnapshot'
  -> RaftState
  -> m ()
handleInstallSnapshot is s
  | is ^. #term < s ^. #currTerm =
    send (is ^. #leaderId) $ failResponse s
  | otherwise = do
    let snapIndex = is ^. #lastIncludedIndex
        snapTerm  = is ^. #lastIncludedTerm
        offset    = is ^. #offset
    when (offset == 0) $ createSnapshot snapIndex snapTerm
    writeSnapshot offset (is ^. #data) snapIndex

    when (is ^. #done) $ do
      saveSnapshot snapIndex
      loadEntry snapIndex >>= \case
        Just e | entryTerm e == is ^. #lastIncludedTerm -> do
          -- Delete logs up to index.
          first <- firstIndex
          deleteRange first snapIndex
        _ -> do
          -- Discard entire log and reset state machine
          -- using snapshot contents.
          first <- firstIndex
          last  <- lastIndex
          deleteRange first last

          snap <- readSnapshot snapIndex
          traverse_ (loadSnapshot snapIndex snapTerm) snap

    send (is ^. #leaderId) $ successResponse s
 where
  successResponse s =
    Response (s ^. #serverID) $ InstallSnapshotResponse $ s ^. #currTerm
  failResponse s =
    Response (s ^. #serverID) $ InstallSnapshotResponse $ s ^. #currTerm
{-# INLINABLE handleInstallSnapshot #-}