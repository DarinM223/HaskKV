{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Raft.Class where

import Control.Monad.State (MonadIO (..), MonadTrans (..), MonadState, void)
import HaskKV.Raft.State
import HaskKV.Utils (persistBinary)
import Optics ((<&>), (^.), guse)
import System.Log.Logger (debugM)

class DebugM m where
  debug :: String -> m ()

newtype PrintDebugT m a = PrintDebugT { unPrintDebugT :: m a }
  deriving (Functor, Applicative, Monad)

instance MonadTrans PrintDebugT where
  lift = PrintDebugT

instance (MonadIO m, MonadState RaftState m) => DebugM (PrintDebugT m) where
  debug text = do
    sid       <- lift $ guse #serverID
    stateText <- lift (guse #stateType) <&> \case
      Follower    -> "Follower"
      Candidate _ -> "Candidate"
      Leader    _ -> "Leader"
    let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
    lift $ liftIO $ debugM (show sid) (serverName ++ text)

class PersistM m where
  persist :: RaftState -> m ()

newtype PersistT m a = PersistT { unPersistT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO)

instance (MonadIO m) => PersistM (PersistT m) where
  persist state = void <$> liftIO $ persistBinary
    persistentStateFilename
    (state ^. #serverID)
    (newPersistentState state)
