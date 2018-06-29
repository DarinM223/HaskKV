module HaskKV.Raft.Leader where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import Data.List
import GHC.Records
import HaskKV.Log
import HaskKV.Log.Utils
import HaskKV.Raft.Debug
import HaskKV.Raft.Message
import HaskKV.Raft.RPC
import HaskKV.Raft.State
import HaskKV.Raft.Utils
import HaskKV.Server
import HaskKV.Snapshot

import qualified Data.IntMap as IM

runLeader :: ( DebugM m
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
        lastIndex' <- lastIndex
        stateType._Leader.matchIndex %= IM.insert serverID' lastIndex'
        ids <- serverIds
        let otherServerIds = filter (/= serverID') ids
        debug "Sending AppendEntries"
        mapM_ (sendAppendEntries lastIndex' commitIndex') otherServerIds
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

handleLeaderResponse sender msg@(InstallSnapshotResponse term) s
    | term < _currTerm s = return ()
    | term > _currTerm s = transitionToFollower msg
    | otherwise = do
        hasRemaining <- hasChunk sender
        if hasRemaining
            then do
                chunk <- readChunk snapshotChunkSize sender
                mapM_ (sendSnapshotChunk sender) chunk
            else snapshotInfo >>= \info -> forM_ info $ \(i, _) -> do
                stateType._Leader.matchIndex %= IM.adjust (max i) sender
                stateType._Leader.nextIndex %= IM.adjust (max (i + 1)) sender

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

storeTemporaryEntries :: ( DebugM m
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
                     )
                  => Int
                  -> Int
                  -> Int
                  -> m ()
sendAppendEntries lastIndex commitIndex id = do
    nextIndexes <- preuse (stateType._Leader.nextIndex)
    case nextIndexes >>= IM.lookup id of
        Just nextIndex -> do
            let pi = prevIndex nextIndex
            pt <- termFromIndex $ prevIndex nextIndex
            entries <- entryRange nextIndex lastIndex
            if isNothing entries || isNothing pt
                then tryInstallSnapshot id pi pt commitIndex
                else sendAppend id pi pt (fromMaybe [] entries) commitIndex
        Nothing -> sendAppend id 0 Nothing [] commitIndex
  where
    sendAppend id prevIndex prevTerm entries commitIndex = do
        term <- use currTerm
        sid <- use serverID
        send id AppendEntries
            { _term        = term
            , _leaderId    = sid
            , _prevLogIdx  = prevIndex
            , _prevLogTerm = fromMaybe 0 prevTerm
            , _entries     = entries
            , _commitIdx   = commitIndex
            }
    tryInstallSnapshot id pi pt commitIndex =
        readChunk snapshotChunkSize id >>= \case
            Just chunk -> sendSnapshotChunk id chunk
            Nothing    -> sendAppend id pi pt [] commitIndex

sendSnapshotChunk :: (MonadState RaftState m, ServerM (RaftMessage e) event m)
                  => Int
                  -> SnapshotChunk
                  -> m ()
sendSnapshotChunk id chunk = do
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
