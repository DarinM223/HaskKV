module HaskKV.Raft.Utils where

import Control.Monad.State
import Data.Maybe
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Types
import Optics
import Optics.State.Operators

import qualified Data.IntMap as IM

transitionToFollower
  :: (MonadState RaftState m, LabelOptic' "term" A_Lens msg LogTerm, PersistM m)
  => msg
  -> m ()
transitionToFollower msg = do
  #stateType .= Follower
  setCurrTerm $ msg ^. #term
  get >>= persist

transitionToLeader
  :: ( LogM (LogEntry k v) m
     , MonadState RaftState m
     , ServerM (RaftMessage (LogEntry k v)) ServerEvent m
     , LabelOptic' "term" A_Lens msg LogTerm
     )
  => msg
  -> m ()
transitionToLeader msg = do
  reset HeartbeatTimeout

  prevLastIndex <- lastIndex
  currTerm'     <- guse #currTerm
  let noop = LogEntry
        { term      = currTerm'
        , index     = prevLastIndex + 1
        , entryData = Noop
        , completed = Completed Nothing
        }
  storeEntries [noop]

  sid          <- guse #serverID
  prevLastTerm <- fromMaybe 0 <$> termFromIndex prevLastIndex
  broadcast $ AppendEntries $ AppendEntries'
    { aeTerm        = msg ^. #term
    , aeLeaderId    = sid
    , aePrevLogIdx  = prevLastIndex
    , aePrevLogTerm = prevLastTerm
    , aeEntries     = [noop]
    , aeCommitIdx   = 0
    }

  ids           <- fmap unSID <$> serverIds
  initNextIndex <- (+ 1) <$> lastIndex
  let nextIndexes  = IM.fromList . fmap (, initNextIndex) $ ids
      matchIndexes = IM.fromList . fmap (, 0) $ ids
      leaderState  = LeaderState { leaderStateNextIndex  = nextIndexes
                                 , leaderStateMatchIndex = matchIndexes }
  #stateType .= Leader leaderState

quorumSize :: (ServerM msg e m) => m Int
quorumSize = do
  servers <- length <$> serverIds
  return $ servers `quot` 2 + 1

startElection
  :: ( MonadState RaftState m
     , LogM e m
     , ServerM (RaftMessage e) event m
     , PersistM m
     )
  => m ()
startElection = do
  sid <- guse #serverID
  #stateType .= Candidate 1
  updateCurrTerm (+ 1)
  #votedFor .= Just sid
  get >>= persist

  lastIndex' <- lastIndex
  lastTerm   <- fromMaybe 0 <$> termFromIndex lastIndex'

  term <- guse #currTerm
  broadcast $ RequestVote $ RequestVote'
    { rvCandidateID = sid
    , rvTerm        = term
    , rvLastLogIdx  = lastIndex'
    , rvLastLogTerm = lastTerm
    }

setCurrTerm :: (MonadState RaftState m) => LogTerm -> m ()
setCurrTerm term = do
  #currTerm .= term
  #votedFor .= Nothing

updateCurrTerm :: (MonadState RaftState m) => (LogTerm -> LogTerm) -> m ()
updateCurrTerm f = do
  #currTerm %= f
  #votedFor .= Nothing
