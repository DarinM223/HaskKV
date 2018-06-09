module HaskKV.Raft.RPC where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Utils
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server
import HaskKV.Snapshot
import HaskKV.Store

handleRequestVote :: ( MonadIO m
                     , ServerM (RaftMessage e) ServerEvent m
                     , MonadState RaftState m
                     , LogM e m
                     , Entry e
                     )
                  => RaftMessage e
                  -> RaftState
                  -> m ()
handleRequestVote rv s
    | existingLeader rv s                = fail rv s
    | getField @"_term" rv < _currTerm s = fail rv s
    | getField @"_term" rv > _currTerm s = do
        debug "Transitioning to follower"
        transitionToFollower rv
        get >>= handleRequestVote rv
    | canVote (_candidateID rv) s = do
        lastEntry <- lastIndex >>= loadEntry
        let validLog = maybe (True, True) (checkValid rv) lastEntry
        if validLog == (True, True)
            then do
                debug $ "Sending vote to " ++ show (_candidateID rv)
                send (_candidateID rv) $ successResponse s
                votedFor .= Just (_candidateID rv)
                reset ElectionTimeout
            else
                fail rv s
    | otherwise = fail rv s
  where
    successResponse s = Response (_serverID s) $ VoteResponse (_currTerm s) True
    failResponse s = Response (_serverID s) $ VoteResponse (_currTerm s) False

    existingLeader rv s = _leader s /= Nothing
                       && _leader s /= Just (_candidateID rv)
    canVote cid s = _votedFor s == Nothing || _votedFor s == Just cid
    checkValid rv e = (checkIndex rv e, checkTerm rv e)
    checkIndex rv e = _lastLogIdx rv >= entryIndex e
    checkTerm rv e = _lastLogTerm rv >= entryTerm e
    fail rv = send (_candidateID rv) . failResponse

handleAppendEntries :: ( MonadIO m
                       , ServerM (RaftMessage e) ServerEvent m
                       , MonadState RaftState m
                       , LogM e m
                       , Entry e
                       )
                    => RaftMessage e
                    -> RaftState
                    -> m ()
handleAppendEntries ae s
    | getField @"_term" ae < _currTerm s =
        send (_leaderId ae) $ failResponse s
    | getField @"_term" ae > _currTerm s = do
        debug "Transitioning to follower"
        transitionToFollower ae
        get >>= handleAppendEntries ae
    | otherwise = do
        leader .= Just (_leaderId ae)
        reset ElectionTimeout

        prevLogTerm <- fmap (maybe 0 entryTerm) . loadEntry . _prevLogIdx $ ae
        if prevLogTerm == _prevLogTerm ae
            then do
                lastLogIndex <- lastIndex
                newEntries <- diffEntriesWithLog lastLogIndex
                            . getField @"_entries"
                            $ ae
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
            else
                send (_leaderId ae) $ failResponse s
  where
    successResponse lastIndex s = Response (_serverID s)
                                $ AppendResponse (_currTerm s) True lastIndex
    failResponse s = Response (_serverID s)
                   $ AppendResponse (_currTerm s) False 0

handleInstallSnapshot :: ( StorageM k v m
                         , LogM e m
                         , ServerM (RaftMessage e) ServerEvent m
                         , SnapshotM s m
                         , LoadSnapshotM s m
                         , MonadState RaftState m
                         , Entry e
                         )
                      => RaftMessage e
                      -> RaftState
                      -> m ()
handleInstallSnapshot is s
    | getField @"_term" is < _currTerm s =
        send (_leaderId is) $ failResponse s
    | otherwise = do
        let snapIndex = _lastIncludedIndex is
        when (_offset is == 0) $ createSnapshot snapIndex

        when (_done is) $ do
            saveSnapshot snapIndex
            loadEntry snapIndex >>= \case
                Just e | entryIndex e == snapIndex
                      && entryTerm e == _lastIncludedTerm is ->
                    -- TODO(DarinM223): delete logs up to index
                    return ()
                _ -> do
                    -- Discard entire log and reset state machine
                    -- using snapshot contents.
                    first <- firstIndex
                    last <- lastIndex
                    deleteRange first last

                    snap <- readSnapshot snapIndex
                    mapM_ loadSnapshot snap

        send (_leaderId is) $ successResponse s
  where
    successResponse s = Response (_serverID s)
                      $ InstallSnapshotResponse (_currTerm s)
    failResponse s = Response (_serverID s)
                   $ InstallSnapshotResponse (_currTerm s)
