module HaskKV.Raft.Leader where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server

import qualified Data.IntMap as IM

runLeader :: ( MonadIO m
             , MonadState RaftState m
             , LogM e m
             , ServerM (RaftMessage e) ServerEvent m
             , Entry e
             )
          => m ()
runLeader = do
    msg <- recv
    case msg of
        Left ElectionTimeout -> reset ElectionTimeout
        Left HeartbeatTimeout -> do
            reset HeartbeatTimeout
            commitIndex' <- use commitIndex
            lastEntry <- lastIndex >>= loadEntry
            serverID' <- use serverID

            stateType._Leader.matchIndex %= ( IM.insert serverID'
                                            . maybe 0 entryIndex
                                            $ lastEntry
                                            )
            ids <- serverIds
            let otherServerIds = filter (/= serverID') ids
            mapM_ (sendAppendEntries lastEntry commitIndex') otherServerIds
        Right rv@RequestVote{}   -> get >>= handleRequestVote rv
        Right ae@AppendEntries{} -> get >>= handleAppendEntries ae
        Right resp@Response{}    -> get >>= handleLeaderResponse resp
    return ()

handleLeaderResponse = undefined

sendAppendEntries :: ( MonadState RaftState m
                     , LogM e m
                     , ServerM (RaftMessage e) ServerEvent m
                     , Entry e
                     )
                  => Maybe e
                  -> Int
                  -> Int
                  -> m ()
sendAppendEntries entry commitIndex' id = do
    currTerm' <- use currTerm
    nextIndexes <- preuse (stateType._Leader.nextIndex)

    let lastIndex = maybe 0 entryIndex entry
        lastTerm  = maybe 0 entryTerm entry
        nextIndex = IM.lookup id =<< nextIndexes

    entries <- case nextIndex of
            Just ni -> fromMaybe [] <$> getEntries [] lastIndex ni
            Nothing -> return []

    serverID' <- use serverID
    if null entries
        then send id AppendEntries
            { _term        = currTerm'
            , _leaderId    = serverID'
            , _prevLogIdx  = lastIndex
            , _prevLogTerm = lastTerm
            , _entries     = []
            , _commitIdx   = commitIndex'
            }
        else do
            let firstSendingIdx = entryIndex $ head entries
            prevEntry <- loadEntry $ prevIndex firstSendingIdx
            let prevLogIdx  = maybe 0 entryIndex prevEntry
                prevLogTerm = maybe 0 entryTerm prevEntry
            send id AppendEntries
                { _term        = currTerm'
                , _leaderId    = serverID'
                , _prevLogIdx  = prevLogIdx
                , _prevLogTerm = prevLogTerm
                , _entries     = entries
                , _commitIdx   = commitIndex'
                }
  where
    getEntries :: (Entry e, LogM e m) => [e] -> Int -> Int -> m (Maybe [e])
    getEntries entries index nextIndex
        | index <= nextIndex = return $ Just entries
        | otherwise = do
            entry <- loadEntry index
            case entry of
                Just e  -> getEntries (e:entries) (prevIndex index) nextIndex
                Nothing -> return Nothing

prevIndex :: Int -> Int
prevIndex index = if index <= 0 then 0 else index - 1
