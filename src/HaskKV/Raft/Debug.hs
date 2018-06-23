{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Raft.Debug where

import Control.Lens
import Control.Monad.State
import HaskKV.Raft.State
import System.Log.Logger

class DebugM m where
    debug :: String -> m ()

newtype PrintDebugT m a = PrintDebugT { unPrintDebugT :: m a }
    deriving (Functor, Applicative, Monad)

instance MonadTrans PrintDebugT where
    lift = PrintDebugT

instance (MonadIO m, MonadState RaftState m) => DebugM (PrintDebugT m) where
    debug text = do
        sid <- lift $ use serverID
        stateText <- lift (use stateType) >>= pure . \case
            Follower    -> "Follower"
            Candidate _ -> "Candidate"
            Leader _    -> "Leader"
        let serverName = "Server " ++ show sid ++ " [" ++ stateText ++ "]:"
        lift $ liftIO $ debugM (show sid) (serverName ++ text)
