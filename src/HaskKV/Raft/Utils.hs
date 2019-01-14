module HaskKV.Raft.Utils where

import Control.Lens
import Control.Monad.State
import Data.Maybe
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Raft.Class
import HaskKV.Raft.Message
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM

transitionToFollower
  :: (MonadState RaftState m, HasField "_term" msg LogTerm)
  => PersistM m
  -> msg
  -> m ()
transitionToFollower persistM msg = do
  stateType .= Follower
  setCurrTerm $ getField @"_term" msg
  get >>= persist persistM

type Msg k v = RaftMessage (LogEntry k v)

transitionToLeader
  :: ( MonadState RaftState m
     , HasLogM (LogEntry k v) m effs
     , HasServerM (RaftMessage (LogEntry k v)) ServerEvent m effs
     , HasField "_term" msg LogTerm )
  => effs
  -> msg
  -> m ()
transitionToLeader effs msg = do
  let serverM = getServerM effs
      logM    = getLogM effs
  reset serverM HeartbeatTimeout

  prevLastIndex <- lastIndex logM
  currTerm'     <- use currTerm
  let
    noop = LogEntry
      { _term      = currTerm'
      , _index     = prevLastIndex + 1
      , _data      = Noop
      , _completed = Completed Nothing
      }
  storeEntries logM [noop]

  sid          <- use serverID
  prevLastTerm <- fromMaybe 0 <$> termFromIndex logM prevLastIndex
  broadcast serverM $ AppendEntries
    { _term        = getField @"_term" msg
    , _leaderId    = sid
    , _prevLogIdx  = prevLastIndex
    , _prevLogTerm = prevLastTerm
    , _entries     = [noop]
    , _commitIdx   = 0
    }

  ids           <- fmap unSID <$> serverIds serverM
  initNextIndex <- (+ 1) <$> lastIndex logM
  let
    nextIndexes  = IM.fromList . fmap (, initNextIndex) $ ids
    matchIndexes = IM.fromList . fmap (, 0) $ ids
    leaderState =
      LeaderState {_nextIndex = nextIndexes, _matchIndex = matchIndexes}
  stateType .= Leader leaderState

quorumSize :: Monad m => ServerM msg e m -> m Int
quorumSize serverM = do
  servers <- length <$> serverIds serverM
  return $ servers `quot` 2 + 1

startElection :: ( MonadState RaftState m
                 , HasLogM e m effs
                 , HasServerM (RaftMessage e) event m effs
                 , HasPersistM m effs )
              => effs -> m ()
startElection effs = do
  let persistM = getPersistM effs
      logM     = getLogM effs
      serverM  = getServerM effs
  sid <- use serverID
  stateType .= Candidate 1
  updateCurrTerm (+ 1)
  votedFor .= Just sid
  get >>= persist persistM

  lastIndex' <- lastIndex logM
  lastTerm   <- fromMaybe 0 <$> termFromIndex logM lastIndex'

  term       <- use currTerm
  broadcast serverM RequestVote
    { _candidateID = sid
    , _term        = term
    , _lastLogIdx  = lastIndex'
    , _lastLogTerm = lastTerm
    }

setCurrTerm :: MonadState RaftState m => LogTerm -> m ()
setCurrTerm term = do
  currTerm .= term
  votedFor .= Nothing

updateCurrTerm :: MonadState RaftState m => (LogTerm -> LogTerm) -> m ()
updateCurrTerm f = do
  currTerm %= f
  votedFor .= Nothing
