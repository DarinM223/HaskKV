{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
module Lib where

import Control.Concurrent (forkIO)
import Control.Monad (forever)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (MaybeT), runMaybeT)
import Data.Foldable (traverse_)
import HaskKV
import HaskKV.Store.Types (LoadSnapshotM (loadSnapshot))
import Optics ((^.))
import Servant.Server (hoistServer, serve)
import System.Log.Logger
import System.Log.Handler.Simple (fileHandler)
import System.Log.Handler (setFormatter)
import System.Log.Formatter (simpleLogFormatter)

import qualified Network.Wai.Handler.Warp as Warp

type MyKey     = Int
type MyValue   = StoreValue Int
type MyEntry   = LogEntry MyKey MyValue
type MyMessage = RaftMessage MyEntry
type MyConfig  = AppConfig MyMessage MyKey MyValue MyEntry

setLogging :: String -> IO ()
setLogging sid = do
  updateGlobalLogger sid       (setLevel DEBUG)
  updateGlobalLogger "conduit" (setLevel DEBUG)
  let template = "[$time : $loggername : $prio] $msg"
  h <- fileHandler (sid ++ ".out") DEBUG
    >>= \lh -> return $ setFormatter lh (simpleLogFormatter template)
  updateGlobalLogger sid       (addHandler h)
  updateGlobalLogger "conduit" (addHandler h)

handleArgs :: [String] -> IO ()
handleArgs (path : sid : _) = do
  setLogging sid

  let
    sid'   = SID $ read sid
    config = Config
      { backpressure     = Capacity 100
      , electionTimeout  = 2000000
      , heartbeatTimeout = 1000000
      , serverData       = []
      }
  config <- readConfig config path
  let
    raftPort = configRaftPort sid' config
    apiPort  = configAPIPort sid' config
    settings = configToSettings config
  initAppConfig <-
    InitAppConfig
    <$> loadBinary logFilename             sid'
    <*> loadBinary persistentStateFilename sid'
    <*> configToServerState sid' config
    <*> pure (configSnapshotDirectory sid' config)
  appConfig <- newAppConfig initAppConfig :: IO MyConfig
  flip runApp appConfig $ runMaybeT $ do
    (index, term, _) <- MaybeT snapshotInfo
    snapshot         <- MaybeT (readSnapshot index)
    lift $ loadSnapshot index term snapshot

  -- Run Raft server and handler.
  traverse_
    (\p -> runServer p "*" settings (appConfig ^. serverStateL))
    raftPort
  forkIO $ forever $ runApp runRaft appConfig

  -- Run API server.
  let apiServer = hoistServer api (convertApp appConfig) server
  traverse_ (`Warp.run` serve api apiServer) apiPort
handleArgs _ = do
  putStrLn "Invalid arguments passed"
  putStrLn "Arguments are:"
  putStrLn "[config path] [server id]"
