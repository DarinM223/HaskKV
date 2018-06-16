module HaskKV.Raft.Leader where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import Data.List
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Utils
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server
import HaskKV.Snapshot

import qualified Data.IntMap as IM

runLeader :: ( MonadIO m
             , MonadState RaftState m
             , LogM e m
             , TempLogM e m
             , ServerM (RaftMessage e) ServerEvent m
             , SnapshotM s m
             , Entry e
             )
          => m ()
runLeader = recv >>= \case
    Left ElectionTimeout -> reset ElectionTimeout
    Left HeartbeatTimeout -> do
        reset HeartbeatTimeout
        commitIndex' <- use commitIndex
        serverID' <- use serverID

        storeTemporaryEntries

        -- Update the highest replicated index for our server.
        lastEntry <- lastIndex >>= loadEntry
        stateType._Leader.matchIndex %= ( IM.insert serverID'
                                        . maybe 0 entryIndex
                                        $ lastEntry
                                        )
        ids <- serverIds
        let otherServerIds = filter (/= serverID') ids
        debug "Sending AppendEntries"
        mapM_ (sendAppendEntries lastEntry commitIndex') otherServerIds
    Right rv@RequestVote{}       -> get >>= handleRequestVote rv
    Right ae@AppendEntries{}     -> get >>= handleAppendEntries ae
    Right InstallSnapshot{}      -> return ()
    Right (Response sender resp) -> get >>= handleLeaderResponse sender resp

handleLeaderResponse sender msg@(AppendResponse term success lastIndex) s
    | term < _currTerm s = return ()
    | term > _currTerm s = transitionToFollower msg
    | not success = do
        debug $ "Decrementing next index for server " ++ show sender
        stateType._Leader.nextIndex %= IM.adjust prevIndex sender
    | otherwise = do
        debug $ "Updating indexes for server " ++ show sender
        stateType._Leader.matchIndex %= IM.adjust (max lastIndex) sender
        stateType._Leader.nextIndex %= IM.adjust (max (lastIndex + 1)) sender

        -- If there exists an N such that N > commitIndex,
        -- a majority of matchIndex[i] >= N, and
        -- log[N].term = currentTerm, set commitIndex = N.
        n <- quorumIndex
        debug $ "N: " ++ show n
        debug $ "Commit Index: " ++ show (_commitIndex s)
        when (n > _commitIndex s) $ do
            debug $ "Updating commit index to " ++ show n
            commitIndex .= n

handleLeaderResponse _ _ _ = return ()

quorumIndex :: ( MonadState RaftState m
               , ServerM msg event m
               )
            => m Int
quorumIndex = do
    matchIndexes <- maybe [] IM.elems <$> preuse (stateType._Leader.matchIndex)
    let sorted = sortBy (flip compare) matchIndexes
    quorumSize' <- quorumSize
    return $ sorted !! (quorumSize' - 1)

storeTemporaryEntries :: ( MonadIO m
                         , MonadState RaftState m
                         , LogM e m
                         , TempLogM e m
                         , Entry e
                         )
                      => m ()
storeTemporaryEntries = do
    term <- use currTerm
    lastIndex' <- lastIndex

    entries <- setIndexAndTerms (lastIndex' + 1) term <$> temporaryEntries
    debug $ "Storing temporary entries: " ++ show entries
    storeEntries entries
  where
    setIndexAndTerms _ _ [] = []
    setIndexAndTerms idx term (e:es) = e':rest
      where
        e' = setEntryTerm term . setEntryIndex idx $ e
        rest = setIndexAndTerms (idx + 1) term es

snapshotChunkSize :: Int
snapshotChunkSize = 10

sendAppendEntries :: forall event e s m.
                     ( MonadState RaftState m
                     , LogM e m
                     , ServerM (RaftMessage e) event m
                     , SnapshotM s m
                     , Entry e
                     )
                  => Maybe e
                  -> Int
                  -> Int
                  -> m ()
sendAppendEntries entry commitIndex id = do
    let lastIndex = maybe 0 entryIndex entry
    nextIndexes <- preuse (stateType._Leader.nextIndex)
    case nextIndexes >>= IM.lookup id of
        Just nextIndex -> do
            prevEntry <- loadEntry $ prevIndex nextIndex
            entries <- entryRange nextIndex lastIndex
            if isNothing entries || isNothing prevEntry
                then tryInstallSnapshot id commitIndex
                else sendAppend id prevEntry (fromMaybe [] entries) commitIndex
        Nothing -> sendAppend id (Nothing :: Maybe e) [] commitIndex
  where
    sendAppend id prevEntry entries commitIndex = do
        term <- use currTerm
        sid <- use serverID
        let prevLogIdx  = maybe 0 entryIndex prevEntry
            prevLogTerm = maybe 0 entryTerm prevEntry
        send id AppendEntries
            { _term        = term
            , _leaderId    = sid
            , _prevLogIdx  = prevLogIdx
            , _prevLogTerm = prevLogTerm
            , _entries     = entries
            , _commitIdx   = commitIndex
            }
    tryInstallSnapshot id commitIndex =
        readChunk snapshotChunkSize id >>= \case
            Just chunk -> do
                term <- use currTerm
                sid <- use serverID
                let done      = _type chunk == EndChunk
                    lastIndex = getField @"_index" chunk
                    lastTerm  = getField @"_term" chunk
                    offset    = getField @"_offset" chunk
                    snapData  = getField @"_data" chunk
                send id InstallSnapshot
                    { _term              = term
                    , _leaderId          = sid
                    , _lastIncludedIndex = lastIndex
                    , _lastIncludedTerm  = lastTerm
                    , _offset            = offset
                    , _data              = snapData
                    , _done              = done
                    }
            Nothing -> sendAppend id (Nothing :: Maybe e) [] commitIndex
