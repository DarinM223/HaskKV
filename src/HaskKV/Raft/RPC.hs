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

handleRequestVote :: ( ServerM (RaftMessage e) ServerEvent m
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
        transitionToFollower rv
        get >>= handleRequestVote rv
    | canVote (_candidateID rv) s = do
        lastEntry <- lastIndex >>= loadEntry
        let validLog = maybe (True, True) (checkValid rv) lastEntry
        if validLog == (True, True)
            then do
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

handleAppendEntries :: ( ServerM (RaftMessage e) ServerEvent m
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
        transitionToFollower ae
        get >>= handleAppendEntries ae
    | otherwise = do
        leader .= Just (_leaderId ae)
        prevLogEntry <- loadEntry $ _prevLogIdx ae
        lastLogEntry <- lastIndex >>= loadEntry
        reset ElectionTimeout
        case (prevLogEntry, lastLogEntry) of
            (Just entry, Just lastEntry) | entryTerm entry == _prevLogTerm ae -> do
                newEntries <- diffEntriesWithLog (entryIndex lastEntry)
                            . getField @"_entries"
                            $ ae
                storeEntries newEntries

                let lastEntryIndex = if (null newEntries)
                        then entryIndex lastEntry
                        else entryIndex $ last newEntries
                commitIndex' <- use commitIndex
                when (_commitIdx ae > commitIndex') $
                    commitIndex .= (min lastEntryIndex $ _commitIdx ae)

                send (_leaderId ae) $ successResponse lastEntryIndex s
            _ -> send (_leaderId ae) $ failResponse s
  where
    successResponse lastIndex s = Response (_serverID s)
                                $ AppendResponse (_currTerm s) True lastIndex
    failResponse s = Response (_serverID s)
                   $ AppendResponse (_currTerm s) False 0
