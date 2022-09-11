{-# OPTIONS_GHC -fspecialise-aggressively #-}
{-# LANGUAGE OverloadedStrings #-}
module App where

import Control.Concurrent (forkIO)
import Control.Monad (forever, void)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT, runMaybeT))
import Data.Conduit.Network (ClientSettings)
import Data.Foldable (traverse_)
import Data.IntMap.Strict (IntMap)
import HaskKV
import HaskKV.Store.All

type MyKey     = Int
type MyValue   = StoreValue Int
type MyEntry   = LogEntry MyKey MyValue
type MyMessage = RaftMessage MyEntry
type MyConfig  = AppConfig MyMessage MyKey MyValue MyEntry

app :: MyConfig -> IntMap ClientSettings -> Maybe Int -> IO ()
app appConfig settings raftPort = do
  flip runApp appConfig $ runMaybeT $ do
    (index, term, _) <- MaybeT snapshotInfo
    snapshot         <- MaybeT (readSnapshot index)
    lift $ loadSnapshot index term snapshot

  -- Run Raft server and handler.
  traverse_
    (\p -> runServer p "*" settings (cServerState appConfig))
    raftPort
  void $ forkIO $ forever $ runApp runRaft appConfig