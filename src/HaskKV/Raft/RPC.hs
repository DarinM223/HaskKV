module HaskKV.Raft.RPC where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
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
    | getField @"_term" rv < _currTerm s || _leader s == Nothing =
        send (_candidateID rv) $ Response (_currTerm s) False
    | getField @"_term" rv > _currTerm s = do
        stateType .= Follower
        currTerm .= getField @"_term" rv
        get >>= handleRequestVote rv
    | canVote (_candidateID rv) s = do
        lastIndex' <- lastIndex
        lastEntry <- loadEntry lastIndex'
        let validLog = maybe (True, True) (checkValid rv) lastEntry
        if validLog == (True, True)
            then do
                send (_candidateID rv) $ Response (_currTerm s) True
                votedFor .= Just (_candidateID rv)
                reset ElectionTimeout
            else
                send (_candidateID rv) $ Response (_currTerm s) False
    | otherwise = send (_candidateID rv) $ Response (_currTerm s) False
  where
    canVote cid s = _votedFor s == Nothing || _votedFor s == Just cid
    checkValid rv e = (checkIndex rv e, checkTerm rv e)
    checkIndex rv e = _lastLogIdx rv >= entryIndex e
    checkTerm rv e = _lastLogTerm rv >= entryTerm e

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
        send (_leaderId ae) $ Response (_currTerm s) False
    | getField @"_term" ae > _currTerm s = do
        stateType .= Follower
        currTerm .= getField @"_term" ae
        get >>= handleAppendEntries ae
    | otherwise = do
        leader .= Just (_leaderId ae)
        prevLogEntry <- loadEntry $ _prevLogIdx ae
        lastIndex' <- lastIndex
        lastLogEntry <- loadEntry lastIndex'
        reset ElectionTimeout
        case (prevLogEntry, lastLogEntry) of
            (Just entry, Just lastEntry) | entryTerm entry == _prevLogTerm ae -> do
                entriesStart <- findStart (entryIndex lastEntry)
                              . zip [0..]
                              . getField @"_entries"
                              $ ae
                let newEntries = case entriesStart of
                        Just start -> drop start . getField @"_entries" $ ae
                        Nothing    -> []
                    lastIndex'' = if (null newEntries)
                        then lastIndex'
                        else entryIndex $ last newEntries
                storeEntries newEntries
                commitIndex' <- use commitIndex
                when (_commitIdx ae > commitIndex') $
                    commitIndex .= (min lastIndex'' $ _commitIdx ae)
                send (_leaderId ae) $ Response (_currTerm s) True
            _ -> send (_leaderId ae) $ Response (_currTerm s) False
  where
    findStart :: (LogM e m, Entry e) => Int -> [(Int, e)] -> m (Maybe Int)
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
