module HaskKV.Monad where

import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.IORef
import GHC.Records
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Snapshot
import HaskKV.Snapshot.Types
import HaskKV.Store
import HaskKV.Store.Types
import qualified Data.Map as M

type SnapshotType k v = M.Map k v

data AppConfig msg k v e = AppConfig
  { _store       :: Store k v e
  , _tempLog     :: TempLog e
  , _serverState :: ServerState msg
  , _snapManager :: SnapshotManager
  }

data InitAppConfig msg e = InitAppConfig
  { _initLog       :: Maybe (Log e)
  , _initState     :: Maybe PersistentState
  , _serverState   :: ServerState msg
  , _snapDirectory :: Maybe FilePath
  }

newtype App a = App (ReaderT (IORef RaftState) IO a)
  deriving (Functor, Applicative, Monad, MonadIO)

instance MonadState RaftState App where
  get = App $ ReaderT $ liftIO . readIORef
  put x = App $ ReaderT $ liftIO . flip writeIORef x

runApp :: App a -> IORef RaftState -> IO a
runApp (App m) ref = runReaderT m ref

mkAppState :: InitAppConfig msg (LogEntry k v) -> IO (IORef RaftState)
mkAppState config = newIORef raftState
 where
  serverState = getField @"_serverState" config
  sid = getField @"_sid" serverState
  raftState = mkRaftState sid $ _initState config

mkAppConfig :: InitAppConfig msg (LogEntry k v)
            -> IO (AppConfig msg k v (LogEntry k v))
mkAppConfig config = do
  let serverState = getField @"_serverState" config
      sid         = getField @"_sid" serverState
  store        <- mkStore sid $ _initLog config
  tempLog      <- mkTempLog
  snapManager  <- mkSnapshotManager $ _snapDirectory config
  return AppConfig
    { _store       = store
    , _tempLog     = tempLog
    , _serverState = serverState
    , _snapManager = snapManager
    }
