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
    debug debugM "Transitioning to follower"
    transitionToFollower persistM rv
    get >>= handleRequestVote effs rv
  | canVote (_candidateID rv) s = do
    index <- lastIndex logM
    term  <- termFromIndex logM index
    let isValid = checkValid rv index (fromMaybe 0 term)
    if isValid
      then do
        debug debugM $ "Sending vote to " ++ show (_candidateID rv)
        votedFor .= Just (_candidateID rv)
        get >>= persist persistM
        send serverM (_candidateID rv) $ successResponse s
        reset serverM ElectionTimeout
      else fail rv s
  | otherwise = fail rv s
 where
  serverM = getServerM effs
  logM = getLogM effs
  debugM = getDebugM effs
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
  fail rv = send serverM (_candidateID rv) . failResponse

handleAppendEntries :: ( MonadState RaftState m
                       , HasServerM (RaftMessage e) ServerEvent m effs
                       , HasLogM e m effs
                       , HasPersistM m effs
                       , HasDebugM m effs
                       , Entry e )
                    => effs -> RaftMessage e -> RaftState -> m ()
handleAppendEntries effs ae s
  | getField @"_term" ae < getField @"_currTerm" s =
    send serverM (_leaderId ae) $ failResponse s
  | getField @"_term" ae > getField @"_currTerm" s = do
    debug debugM "Transitioning to follower"
    transitionToFollower persistM ae
    get >>= handleAppendEntries effs ae
  | otherwise = do
    leader .= Just (_leaderId ae)
    reset serverM ElectionTimeout

    prevLogTerm <- termFromIndex logM $ _prevLogIdx ae
    if prevLogTerm == Just (_prevLogTerm ae)
      then do
        lastLogIndex <- lastIndex logM
        newEntries <- diffEntriesWithLog logM lastLogIndex
                    $ getField @"_entries" ae
        storeEntries logM newEntries

        let lastEntryIndex = if (null newEntries)
              then lastLogIndex
              else entryIndex $ last newEntries

        when (lastEntryIndex /= lastLogIndex) $
          debug debugM $ "Storing entries to index " ++ show lastEntryIndex

        commitIndex' <- use commitIndex
        when (_commitIdx ae > commitIndex') $
          commitIndex .= (min lastEntryIndex $ _commitIdx ae)

        send serverM (_leaderId ae) $ successResponse lastEntryIndex s
      else send serverM (_leaderId ae) $ failResponse s
 where
  serverM = getServerM effs
  persistM = getPersistM effs
  logM = getLogM effs
  debugM = getDebugM effs

  successResponse lastIndex s = Response (_serverID s)
    $ AppendResponse (getField @"_currTerm" s) True lastIndex
  failResponse s =
    Response (_serverID s) $ AppendResponse (getField @"_currTerm" s) False 0

handleInstallSnapshot :: ( Monad m
                         , HasStorageM k v m effs
                         , HasLogM e m effs
                         , HasServerM (RaftMessage e) ServerEvent m effs
                         , HasSnapshotM s m effs
                         , HasLoadSnapshotM s m effs
                         , Entry e )
                      => effs -> RaftMessage e -> RaftState -> m ()
handleInstallSnapshot effs is s
  | getField @"_term" is < getField @"_currTerm" s =
    send serverM (_leaderId is) $ failResponse s
  | otherwise = do
    let snapIndex = _lastIncludedIndex is
        snapTerm  = _lastIncludedTerm is
        offset    = getField @"_offset" is
    when (offset == 0) $ createSnapshot snapM snapIndex snapTerm
    writeSnapshot snapM offset (getField @"_data" is) snapIndex

    when (_done is) $ do
      saveSnapshot snapM snapIndex
      loadEntry logM snapIndex >>= \case
        Just e | entryTerm e == _lastIncludedTerm is -> do
          -- Delete logs up to index.
          first <- firstIndex logM
          deleteRange logM first snapIndex
        _ -> do
          -- Discard entire log and reset state machine
          -- using snapshot contents.
          first <- firstIndex logM
          last  <- lastIndex logM
          deleteRange logM first last

          snap <- readSnapshot snapM snapIndex
          mapM_ (loadSnapshot loadSnapM snapIndex snapTerm) snap

    send serverM (_leaderId is) $ successResponse s
 where
  serverM = getServerM effs
  snapM = getSnapshotM effs
  loadSnapM = getLoadSnapshotM effs
  logM = getLogM effs

  successResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
  failResponse s =
    Response (_serverID s) $ InstallSnapshotResponse (getField @"_currTerm" s)
