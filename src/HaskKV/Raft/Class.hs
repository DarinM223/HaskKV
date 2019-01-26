{-# LANGUAGE TemplateHaskell #-}

module HaskKV.Raft.Class where

import Control.Lens
import Control.Monad.State
import GHC.Records
import HaskKV.Log.Class
import HaskKV.Log.Entry
import HaskKV.Log.Temp
import HaskKV.Monad
import HaskKV.Raft.State
import HaskKV.Server.Types
import HaskKV.Server
import HaskKV.Snapshot.Types
import HaskKV.Snapshot
import HaskKV.Store
import HaskKV.Store.Types
import HaskKV.Utils
import System.Log.Logger
import qualified Data.Map as M

newtype DebugM m = DebugM { debug :: String -> m () }

debug' text = do
  sid       <- use serverID
  stateText <- use stateType >>= pure . \case
    Follower    -> "Follower"
    Candidate _ -> "Candidate"
    Leader    _ -> "Leader"
  let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
  liftIO $ debugM (show sid) (serverName ++ text)

newtype PersistM m = PersistM { persist :: RaftState -> m () }

persist' state = void <$> liftIO $ persistBinary
  persistentStateFilename
  (_serverID state)
  (mkPersistentState state)

data Effs k v e s msg event m = Effs
  { _storageM      :: StorageM k v m
  , _logM          :: LogM e m
  , _tempLogM      :: TempLogM e m
  , _applyEntryM   :: ApplyEntryM e m
  , _serverM       :: ServerM msg event m
  , _snapshotM     :: SnapshotM s m
  , _loadSnapshotM :: LoadSnapshotM s m
  , _persistM      :: PersistM m
  , _debugM        :: DebugM m
  }
makeFieldsNoPrefix ''Effs

mkEffs :: (MonadIO m, MonadState RaftState m, KeyClass k, ValueClass v)
       => AppConfig msg k v (LogEntry k v)
       -> (forall a. m a -> IO a)
       -> Effs k v (LogEntry k v) (M.Map k v) msg ServerEvent m
mkEffs cfg run = Effs
  { _storageM      = storeM
  , _logM          = mkLogM snapM takeSnapM $ _store cfg
  , _tempLogM      = mkTempLogM $ _tempLog cfg
  , _applyEntryM   = ApplyEntryM $ applyEntry' storeM
  , _serverM       = mkServerM $ getField @"_serverState" cfg
  , _snapshotM     = snapM
  , _loadSnapshotM = LoadSnapshotM $ loadSnapshot' $ _store cfg
  , _persistM      = PersistM persist'
  , _debugM        = DebugM debug'
  }
 where
  snapM = mkSnapshotM $ _snapManager cfg
  takeSnapM = TakeSnapshotM $ takeSnapshot' snapM run $ _store cfg
  storeM = mkStorageM $ _store cfg
