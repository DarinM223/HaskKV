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

handleRequestVote :: ( MonadState RaftState m
                     , HasServerM (RaftMessage e) ServerEvent m effs
                     , HasLogM e m effs
                     , HasDebugM m effs
                     , HasPersistM m effs
                     , Entry e )
                  => effs -> RaftMessage e -> RaftState -> m ()
handleRequestVote effs rv s
  | existingLeader rv s = fail rv s
  | getField @"_term" rv < getField @"_currTerm" s = fail rv s
  | getField @"_term" rv > getField @"_currTerm" s = do
    debug "Transitioning to follower"
    transitionToFollower persistM rv
    get >>= handleRequestVote effs rv
  | canVote (_candidateID rv) s = do
    index <- lastIndex
    term  <- termFromIndex index
    let isValid = checkValid rv index (fromMaybe 0 term)
    if isValid
      then do
        debug $ "Sending vote to " ++ show (_candidateID rv)
        votedFor .= Just (_candidateID rv)
        get >>= persist persistM
        send (_candidateID rv) $ successResponse s
        reset ElectionTimeout
      else fail rv s
  | otherwise = fail rv s
 where
  ServerM { reset, send } = getServerM effs
  LogM { lastIndex, termFromIndex } = getLogM effs
  DebugM debug = getDebugM effs
  persistM = getPersistM effs

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

handleAppendEntries :: ( MonadState RaftState m
                       , HasServerM (RaftMessage e) ServerEvent m effs
                       , HasLogM e m effs
                       , HasPersistM m effs
                       , HasDebugM m effs
                       , Entry e )
                    => effs -> RaftMessage e -> RaftState -> m ()
handleAppendEntries effs ae s
  | getField @"_term" ae < getField @"_currTerm" s =
    send (_leaderId ae) $ failResponse s
  | getField @"_term" ae > getField @"_currTerm" s = do
    debug "Transitioning to follower"
    transitionToFollower persistM ae
    get >>= handleAppendEntries effs ae
  | otherwise = do
    leader .= Just (_leaderId ae)
    reset ElectionTimeout

    prevLogTerm <- termFromIndex $ _prevLogIdx ae
    if prevLogTerm == Just (_prevLogTerm ae)
      then do
        lastLogIndex <- lastIndex
        newEntries <- diffEntriesWithLog logM lastLogIndex
                    $ getField @"_entries" ae
        storeEntries newEntries

        let lastEntryIndex = if (null newEntries)
              then lastLogIndex
              else entryIndex $ last newEntries

        when (lastEntryIndex /= lastLogIndex) $
          debug $ "Storing entries to index " ++ show lastEntryIndex

        commitIndex' <- use commitIndex
        when (_commitIdx ae > commitIndex') $
          commitIndex .= (min lastEntryIndex $ _commitIdx ae)

        send (_leaderId ae) $ successResponse lastEntryIndex s
      else send (_leaderId ae) $ failResponse s
 where
  ServerM { reset, send } = getServerM effs
  persistM = getPersistM effs
  logM@LogM { lastIndex, storeEntries, termFromIndex } = getLogM effs
  DebugM debug = getDebugM effs

  successResponse lastIndex s = Response (_serverID s)
    $ AppendResponse (getField @"_currTerm" s) True lastIndex
  failResponse s =
    Response (_serverID s) $ AppendResponse (getField @"_currTerm" s) False 0

handleInstallSnapshot :: ( Monad m
                         , HasLogM e m effs
                         , HasServerM (RaftMessage e) ServerEvent m effs
                         , HasSnapshotM s m effs
                         , HasLoadSnapshotM s m effs
                         , Entry e )
                      => effs -> RaftMessage e -> RaftState -> m ()
handleInstallSnapshot effs is s
  | getField @"_term" is < getField @"_currTerm" s =
    send (_leaderId is) $ failResponse s
  | otherwise = do
    let snapIndex = _lastIncludedIndex is
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
  ServerM { send } = getServerM effs
  SnapshotM { createSnapshot, readSnapshot, saveSnapshot, writeSnapshot }
    = getSnapshotM effs
  LoadSnapshotM loadSnapshot = getLoadSnapshotM effs
  LogM { deleteRange, firstIndex, lastIndex, loadEntry }= getLogM effs

  successResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
  failResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
