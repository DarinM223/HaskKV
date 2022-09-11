{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Raft.Class where

import Control.Monad.State (MonadIO (..), MonadState)
import HaskKV.Raft.State
import Optics ((<&>), guse)
import System.Log.Logger (debugM)

class DebugM m where
  debug :: String -> m ()

newtype PrintDebugT m a = PrintDebugT (m a)
  deriving (Functor, Applicative, Monad)

newtype NoDebugT m a = NoDebugT (m a)
  deriving (Functor, Applicative, Monad)

instance Monad m => DebugM (NoDebugT m) where
  debug _ = return ()

instance (MonadIO m, MonadState RaftState m) => DebugM (PrintDebugT m) where
  debug text = PrintDebugT $ do
    sid       <- guse #serverID
    stateText <- guse #stateType <&> \case
      Follower    -> "Follower"
      Candidate _ -> "Candidate"
      Leader    _ -> "Leader"
    let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
    liftIO $ debugM (show sid) (serverName ++ text)

class PersistM m where
  persist :: RaftState -> m ()
