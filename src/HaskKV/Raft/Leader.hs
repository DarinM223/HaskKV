module HaskKV.Raft.Leader where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import Data.List
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

            -- Update the highest replicated index for our server.
            stateType._Leader.matchIndex %= ( IM.insert serverID'
                                            . maybe 0 entryIndex
                                            $ lastEntry
                                            )
            ids <- serverIds
            let otherServerIds = filter (/= serverID') ids
            mapM_ (sendAppendEntries lastEntry commitIndex') otherServerIds
        Right rv@RequestVote{}       -> get >>= handleRequestVote rv
        Right ae@AppendEntries{}     -> get >>= handleAppendEntries ae
        Right (Response sender resp) -> get >>= handleLeaderResponse sender resp

handleLeaderResponse sender msg@(AppendResponse term success lastIndex) s
    | term < _currTerm s = return ()
    | term > _currTerm s = transitionToFollower msg
    | not success =
        stateType._Leader.nextIndex %= IM.adjust prevIndex sender
    | otherwise = do
        stateType._Leader.matchIndex %= IM.adjust (max lastIndex) sender
        stateType._Leader.nextIndex %= IM.adjust (max (lastIndex + 1)) sender

        -- If there exists an N such that N > commitIndex,
        -- a majority of matchIndex[i] >= N, and
        -- log[N].term = currentTerm, set commitIndex = N.
        quorumIndex' <- quorumIndex
        when (quorumIndex' > _commitIndex s) $
            commitIndex .= quorumIndex'

handleLeaderResponse _ _ _ = return ()

quorumIndex :: ( MonadState RaftState m
               , ServerM msg event m
               )
            => m Int
quorumIndex = do
    matchIndexes <- maybe [] IM.elems <$> preuse (stateType._Leader.matchIndex)
    quorumSize' <- quorumSize
    let sorted = sortBy (flip compare) matchIndexes
    return $ sorted !! (quorumSize' - 1)

sendAppendEntries :: ( MonadState RaftState m
                     , LogM e m
                     , ServerM (RaftMessage e) event m
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
            Just ni -> fromMaybe [] <$> getEntries [] ni lastIndex
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
    -- Returns a list of entries from nextIndex to lastIndex.
    getEntries :: (Entry e, LogM e m)
               => [e] -- Currently building list. Should be initialized to [].
               -> Int -- The nextIndex of the follower.
               -> Int -- The lastIndex of the leader.
               -> m (Maybe [e])
    getEntries entries nextIndex index
        | index <= nextIndex = return $ Just entries
        | otherwise = do
            entry <- loadEntry index
            case entry of
                Just e  -> getEntries (e:entries) nextIndex (prevIndex index)
                Nothing -> return Nothing

prevIndex :: Int -> Int
prevIndex index = if index <= 0 then 0 else index - 1
