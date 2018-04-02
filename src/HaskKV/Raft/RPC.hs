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
    | getField @"_term" rv > _currTerm s = undefined -- TODO(DarinM223): update term if newer
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
                       , LogM e m
                       , Entry e
                       )
                    => RaftMessage e
                    -> RaftState
                    -> m ()
handleAppendEntries ae s
    | getField @"_term" ae < _currTerm s =
        send (_leaderId ae) $ Response (_currTerm s) False
    | getField @"_term" ae > _currTerm s = undefined -- TODO(DarinM223): update term if newer
    | otherwise = do
        prevLogEntry <- loadEntry $ _prevLogIdx ae
        case prevLogEntry of
            Just entry | entryTerm entry == _prevLogTerm ae -> do
                return ()
            Just entry -> return () -- TODO(DarinM223): delete existing entry and all that follow it
            _ -> undefined
