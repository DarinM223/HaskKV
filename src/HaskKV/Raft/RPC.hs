module HaskKV.Raft.RPC where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Utils
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server.Types
import HaskKV.Snapshot.Types
import HaskKV.Store.Types

handleRequestVote
  :: ( DebugM m
     , ServerM (RaftMessage e) ServerEvent m
     , MonadState RaftState m
     , LogM e m
     , PersistM m
     , Entry e
     )
  => RaftMessage e
  -> RaftState
  -> m ()
handleRequestVote rv s
  | existingLeader rv s = fail rv s
  | getField @"_term" rv < getField @"_currTerm" s = fail rv s
  | getField @"_term" rv > getField @"_currTerm" s = do
    debug "Transitioning to follower"
    transitionToFollower rv
    get >>= handleRequestVote rv
  | canVote (_candidateID rv) s = do
    index <- lastIndex
    term  <- termFromIndex index
    let isValid = checkValid rv index (fromMaybe 0 term)
    if isValid
      then do
        debug $ "Sending vote to " ++ show (_candidateID rv)
        votedFor .= Just (_candidateID rv)
        get >>= persist
        send (_candidateID rv) $ successResponse s
        reset ElectionTimeout
      else fail rv s
  | otherwise = fail rv s
 where
  successResponse s =
    Response (_serverID s) $ VoteResponse (getField @"_currTerm" s) True
  failResponse s =
    Response (_serverID s) $ VoteResponse (getField @"_currTerm" s) False

  existingLeader rv s =
    _leader s /= Nothing && _leader s /= Just (_candidateID rv)
  canVote cid s =
    getField @"_votedFor" s == Nothing || getField @"_votedFor" s == Just cid
  checkValid rv i t = _lastLogIdx rv >= i && _lastLogTerm rv >= t
  fail rv = send (_candidateID rv) . failResponse

handleAppendEntries
  :: ( DebugM m
     , ServerM (RaftMessage e) ServerEvent m
     , MonadState RaftState m
     , LogM e m
     , PersistM m
     , Entry e
     )
  => RaftMessage e
  -> RaftState
  -> m ()
handleAppendEntries ae s
  | getField @"_term" ae < getField @"_currTerm" s = send (_leaderId ae)
  $ failResponse s
  | getField @"_term" ae > getField @"_currTerm" s = do
    debug "Transitioning to follower"
    transitionToFollower ae
    get >>= handleAppendEntries ae
  | otherwise = do
    leader .= Just (_leaderId ae)
    reset ElectionTimeout

    prevLogTerm <- termFromIndex $ _prevLogIdx ae
    if prevLogTerm == Just (_prevLogTerm ae)
      then do
        lastLogIndex <- lastIndex
        newEntries   <-
          diffEntriesWithLog lastLogIndex . getField @"_entries" $ ae
        storeEntries newEntries

        let
          lastEntryIndex = if (null newEntries)
            then lastLogIndex
            else entryIndex $ last newEntries

        when (lastEntryIndex /= lastLogIndex)
          $  debug
          $  "Storing entries to index "
          ++ show lastEntryIndex

        commitIndex' <- use commitIndex
        when (_commitIdx ae > commitIndex')
          $  commitIndex
          .= (min lastEntryIndex $ _commitIdx ae)

        send (_leaderId ae) $ successResponse lastEntryIndex s
      else send (_leaderId ae) $ failResponse s
 where
  successResponse lastIndex s = Response (_serverID s)
    $ AppendResponse (getField @"_currTerm" s) True lastIndex
  failResponse s =
    Response (_serverID s) $ AppendResponse (getField @"_currTerm" s) False 0

handleInstallSnapshot
  :: ( StorageM k v m
     , LogM e m
     , ServerM (RaftMessage e) ServerEvent m
     , SnapshotM s m
     , LoadSnapshotM s m
     , Entry e
     )
  => RaftMessage e
  -> RaftState
  -> m ()
handleInstallSnapshot is s
  | getField @"_term" is < getField @"_currTerm" s = send (_leaderId is)
  $ failResponse s
  | otherwise = do
    let
      snapIndex = _lastIncludedIndex is
      snapTerm  = _lastIncludedTerm is
      offset    = getField @"_offset" is
    when (offset == 0) $ createSnapshot snapIndex snapTerm
    writeSnapshot offset (getField @"_data" is) snapIndex

    when (_done is) $ do
      saveSnapshot snapIndex
      loadEntry snapIndex >>= \case
        Just e | entryTerm e == _lastIncludedTerm is -> do
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
          mapM_ (loadSnapshot snapIndex snapTerm) snap

    send (_leaderId is) $ successResponse s
 where
  successResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
  failResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
