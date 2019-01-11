module HaskKV.Monad where

import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Binary.Orphans ()
import Data.IORef
import GHC.Records
import GHC.Generics
import HaskKV.Constr
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.InMem
import HaskKV.Log.Temp
import HaskKV.Raft.Class
import HaskKV.Raft.State
import HaskKV.Server.All
import HaskKV.Snapshot.All
import HaskKV.Store.All

data AppConfig msg k v e = AppConfig
  { _state       :: IORef RaftState
  , _store       :: Store k v e
  , _tempLog     :: TempLog e
  , _serverState :: ServerState msg
  , _snapManager :: SnapshotManager
  } deriving Generic

data InitAppConfig msg e = InitAppConfig
  { _initLog       :: Maybe (Log e)
  , _initState     :: Maybe PersistentState
  , _serverState   :: ServerState msg
  , _snapDirectory :: Maybe FilePath
  }

instance MonadState RaftState (App msg k v e) where
  get = App $ ReaderT $ liftIO . readIORef . _state
  put x = App $ ReaderT $ liftIO . flip writeIORef x . _state

instance (Binary k, Binary v) =>
  HasSnapshotType (SnapshotType k v) (App msg k v e)

newtype App msg k v e a = App
  { unApp :: ReaderT (AppConfig msg k v e) IO a }
  deriving ( Functor, Applicative, Monad, MonadIO
           , MonadReader (AppConfig msg k v e) )

runApp :: App msg k v e a -> AppConfig msg k v e -> IO a
runApp m config = flip runReaderT config . unApp $ m

newAppConfig
  :: (KeyClass k, ValueClass v, e ~ LogEntry k v)
  => InitAppConfig msg e
  -> IO (AppConfig msg k v e)
newAppConfig config = do
  let
    serverState = getField @"_serverState" config
    sid         = getField @"_sid" serverState
    raftState   = newRaftState sid $ _initState config
  raftStateRef <- newIORef raftState
  store        <- newStore sid $ _initLog config
  tempLog      <- newTempLog
  snapManager  <- newSnapshotManager $ _snapDirectory config
  let
    config = AppConfig
      { _state       = raftStateRef
      , _store       = store
      , _tempLog     = tempLog
      , _serverState = serverState
      , _snapManager = snapManager
      }
  return config
