module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server
import System.Log.Logger

import qualified Data.IntMap as IM

transitionToFollower :: (MonadState RaftState m, HasField "_term" msg Int)
                     => msg
                     -> m ()
transitionToFollower msg = do
    stateType .= Follower
    setCurrTerm $ getField @"_term" msg

transitionToLeader :: ( LogM e m
                      , MonadState RaftState m
                      , ServerM (RaftMessage e) ServerEvent m
                      , Entry e
                      , HasField "_term" msg Int
                      )
                   => msg
                   -> m ()
transitionToLeader msg = do
    reset HeartbeatTimeout
    lastEntry <- lastIndex >>= loadEntry
    serverID' <- use serverID

    let lastEntryIndex = maybe 0 entryIndex lastEntry
        lastEntryTerm  = maybe 0 entryTerm lastEntry

    broadcast $ AppendEntries
        { _term        = getField @"_term" msg
        , _leaderId    = serverID'
        , _prevLogIdx  = lastEntryIndex
        , _prevLogTerm = lastEntryTerm
        , _entries     = []
        , _commitIdx   = 0
        }

    ids <- serverIds
    let initNextIndex = lastEntryIndex + 1
        nextIndexes   = IM.fromList . fmap (flip (,) initNextIndex) $ ids
        matchIndexes  = IM.fromList . fmap (flip (,) 0) $ ids
        leaderState   = LeaderState
            { _nextIndex  = nextIndexes
            , _matchIndex = matchIndexes
            }
    stateType .= Leader leaderState

quorumSize :: (ServerM msg e m) => m Int
quorumSize = do
    servers <- length <$> serverIds
    return $ servers `quot` 2 + 1

startElection :: ( MonadState RaftState m
                 , LogM e m
                 , ServerM (RaftMessage e) event m
                 , Entry e
                 )
              => m ()
startElection = do
    sid <- use serverID
    stateType .= Candidate 1
    updateCurrTerm (+ 1)
    votedFor .= Just sid

    lastEntry <- lastIndex >>= loadEntry
    currTerm' <- use currTerm
    let lastEntryIndex = maybe 0 entryIndex lastEntry
        lastEntryTerm  = maybe 0 entryTerm lastEntry
    broadcast RequestVote
        { _candidateID = sid
        , _term        = currTerm'
        , _lastLogIdx  = lastEntryIndex
        , _lastLogTerm = lastEntryTerm
        }

debug :: (MonadIO m, MonadState RaftState m) => String -> m ()
debug text = do
    sid <- use serverID
    stateType' <- use stateType
    let stateText = case stateType' of
            Follower    -> "Follower"
            Candidate _ -> "Candidate"
            Leader _    -> "Leader"
    let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
    liftIO $ debugM (show sid) (serverName ++ text)

setCurrTerm :: (MonadState RaftState m) => Int -> m ()
setCurrTerm term = do
    currTerm .= term
    votedFor .= Nothing

updateCurrTerm :: (MonadState RaftState m) => (Int -> Int) -> m ()
updateCurrTerm f = do
    currTerm %= f
    votedFor .= Nothing
