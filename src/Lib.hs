module Lib where

import Control.Concurrent (forkIO)
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import GHC.Records
import HaskKV
import HaskKV.Raft.Class
import Servant.Server (serve)
import System.Environment (getArgs)
import System.Log.Logger
import System.Log.Handler.Simple
import System.Log.Handler (setFormatter)
import System.Log.Formatter

import qualified Network.Wai.Handler.Warp as Warp

runProg :: IO ()
runProg = getArgs >>= handleArgs

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

  let sid'   = SID $ read sid
      config = Config
        { _backpressure     = Capacity 100
        , _electionTimeout  = 2000000
        , _heartbeatTimeout = 1000000
        , _serverData       = []
        }
  config <- readConfig config path
  let raftPort = configRaftPort sid' config
      apiPort  = configAPIPort sid' config
      settings = configToSettings config
  initAppConfig <-
    InitAppConfig
    <$> loadBinary logFilename             sid'
    <*> loadBinary persistentStateFilename sid'
    <*> configToServerState sid' config
    <*> pure (configSnapshotDirectory sid' config)
  appConfig <- mkAppConfig initAppConfig :: IO MyConfig
  let effs = mkEffs appConfig (flip runApp appConfig)
      SnapshotM { readSnapshot, snapshotInfo } = _snapshotM effs
      LoadSnapshotM loadSnapshot = _loadSnapshotM effs
  flip runApp appConfig $ runMaybeT $ do
    (index, term, _) <- lift snapshotInfo >>= MaybeT . pure
    snapshot         <- lift (readSnapshot index) >>= MaybeT . pure
    lift $ loadSnapshot index term snapshot

  -- Run Raft server and handler.
  let serverState = getField @"_serverState" appConfig
  mapM_ (\p -> runServer p "*" settings serverState) raftPort
  forkIO $ forever $ runApp (run effs) appConfig

  -- Run API server.
  let server' = server
        (_storageM effs)
        (_serverM effs)
        (_tempLogM effs)
        (flip runApp appConfig)
  mapM_ (flip Warp.run (serve api server')) apiPort
handleArgs _ = do
  putStrLn "Invalid arguments passed"
  putStrLn "Arguments are:"
  putStrLn "[config path] [server id]"
