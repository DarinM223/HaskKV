module HaskKV.Raft.Class where

import Control.Lens
import Control.Monad.State
import HaskKV.Raft.State
import HaskKV.Utils
import System.Log.Logger

newtype DebugM m = DebugM { debug :: String -> m () }

debug' text = do
  sid       <- lift $ use serverID
  stateText <- lift (use stateType) >>= pure . \case
    Follower    -> "Follower"
    Candidate _ -> "Candidate"
    Leader    _ -> "Leader"
  let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
  lift $ liftIO $ debugM (show sid) (serverName ++ text)

newtype PersistM m = PersistM { persist :: RaftState -> m () }

persist' state = void <$> liftIO $ persistBinary
  persistentStateFilename
  (_serverID state)
  (newPersistentState state)

class HasDebugM m cfg | cfg -> m where
  getDebugM :: cfg -> DebugM m
class HasPersistM m cfg | cfg -> m where
  getPersistM :: cfg -> PersistM m
