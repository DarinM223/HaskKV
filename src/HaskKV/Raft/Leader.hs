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
                                            . fromMaybe 0
                                            . fmap entryIndex
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

sendAppendEntries :: e -> Int -> Int -> m ()
sendAppendEntries = undefined
