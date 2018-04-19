module HaskKV.Raft.RPC where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
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
    successResponse s = Response rv (_currTerm s) True (_serverID s)
    failResponse s = Response rv (_currTerm s) False (_serverID s)

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
                entriesStart <- findStart (entryIndex lastEntry)
                              . zip [0..]
                              . getField @"_entries"
                              $ ae

                let newEntries = case entriesStart of
                        Just start -> drop start $ getField @"_entries" ae
                        Nothing    -> []
                storeEntries newEntries

                let lastEntryIndex = if (null newEntries)
                        then entryIndex lastEntry
                        else entryIndex $ last newEntries
                commitIndex' <- use commitIndex
                when (_commitIdx ae > commitIndex') $
                    commitIndex .= (min lastEntryIndex $ _commitIdx ae)

                send (_leaderId ae) $ successResponse s
            _ -> send (_leaderId ae) $ failResponse s
  where
    successResponse s = Response ae (_currTerm s) True (_serverID s)
    failResponse s = Response ae (_currTerm s) False (_serverID s)

    -- Returns the first index in the entries that doesn't exist in the log
    -- or is different from the existing entry in the log.
    --
    -- If there is an existing entry that has a different term, that
    -- entry and all entries after it are cleared from the log.
    findStart :: (LogM e m, Entry e)
              => Int        -- Index of the last entry in the log.
              -> [(Int, e)] -- Entries to append with their indexes.
              -> m (Maybe Int)
    findStart _ [] = return Nothing
    findStart lastIndex ((i, e):es)
        | entryIndex e > lastIndex = return $ Just i
        | otherwise = do
            storeEntry <- loadEntry $ entryIndex e
            case storeEntry of
                Just se | entryTerm e /= entryTerm se -> do
                    deleteRange (entryIndex e) lastIndex
                    return $ Just i
                _ -> findStart lastIndex es
