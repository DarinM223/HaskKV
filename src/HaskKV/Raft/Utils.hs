module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import GHC.Records
import HaskKV.Log
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server
import HaskKV.Types

import qualified Data.IntMap as IM

transitionToFollower :: (MonadState RaftState m, HasField "_term" msg LogTerm)
                     => msg
                     -> m ()
transitionToFollower msg = do
    stateType .= Follower
    setCurrTerm $ getField @"_term" msg

transitionToLeader :: ( LogM e m
                      , MonadState RaftState m
                      , ServerM (RaftMessage e) ServerEvent m
                      , HasField "_term" msg LogTerm
                      )
                   => msg
                   -> m ()
transitionToLeader msg = do
    reset HeartbeatTimeout

    lastIndex' <- lastIndex
    lastTerm <- fromMaybe 0 <$> termFromIndex lastIndex'

    sid <- use serverID
    broadcast $ AppendEntries
        { _term        = getField @"_term" msg
        , _leaderId    = sid
        , _prevLogIdx  = lastIndex'
        , _prevLogTerm = lastTerm
        , _entries     = []
        , _commitIdx   = 0
        }

    ids <- fmap unSID <$> serverIds
    let initNextIndex = lastIndex' + 1
        nextIndexes   = IM.fromList . fmap (, initNextIndex) $ ids
        matchIndexes  = IM.fromList . fmap (, 0) $ ids
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
                 )
              => m ()
startElection = do
    sid <- use serverID
    stateType .= Candidate 1
    updateCurrTerm (+ 1)
    votedFor .= Just sid

    lastIndex' <- lastIndex
    lastTerm <- fromMaybe 0 <$> termFromIndex lastIndex'

    term <- use currTerm
    broadcast RequestVote
        { _candidateID = sid
        , _term        = term
        , _lastLogIdx  = lastIndex'
        , _lastLogTerm = lastTerm
        }

setCurrTerm :: (MonadState RaftState m) => LogTerm -> m ()
setCurrTerm term = do
    currTerm .= term
    votedFor .= Nothing

updateCurrTerm :: (MonadState RaftState m) => (LogTerm -> LogTerm) -> m ()
updateCurrTerm f = do
    currTerm %= f
    votedFor .= Nothing
