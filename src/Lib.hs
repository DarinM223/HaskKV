module Lib where

import Control.Concurrent (forkIO)
import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import GHC.Records
import HaskKV
import HaskKV.Store.All
import Servant.Server (serve)
import System.Environment (getArgs)
import System.Log.Logger
import System.Log.Handler.Simple
import System.Log.Handler (setFormatter)
import System.Log.Formatter

import qualified Network.Wai.Handler.Warp as Warp

runApp :: IO ()
runApp = getArgs >>= handleArgs

type MyKey     = Int
type MyValue   = StoreValue Int
type MyEntry   = LogEntry MyKey MyValue
type MyMessage = RaftMessage MyEntry
type MyConfig  = AppConfig MyMessage MyKey MyValue MyEntry

setLogging :: String -> IO ()
setLogging sid = do
    updateGlobalLogger sid (setLevel DEBUG)
    updateGlobalLogger "conduit" (setLevel DEBUG)
    let template = "[$time : $loggername : $prio] $msg"
    h <- fileHandler (sid ++ ".out") DEBUG >>= \lh ->
        return $ setFormatter lh (simpleLogFormatter template)
    updateGlobalLogger sid (addHandler h)
    updateGlobalLogger "conduit" (addHandler h)

handleArgs :: [String] -> IO ()
handleArgs (path:sid:_) = do
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
    initAppConfig <- InitAppConfig
                 <$> loadBinary logFilename sid'
                 <*> loadBinary persistentStateFilename sid'
                 <*> configToServerState sid' config
                 <*> pure (configSnapshotDirectory sid' config)
    appConfig <- newAppConfig initAppConfig :: IO MyConfig
    flip runAppT appConfig $ runMaybeT $ do
        (index, term, _) <- lift snapshotInfo >>= MaybeT . pure
        snapshot <- lift (readSnapshot index) >>= MaybeT . pure
        lift $ loadSnapshot index term snapshot

    -- Run Raft server and handler.
    let serverState = getField @"_serverState" appConfig
    mapM_ (\p -> runServer p "*" settings serverState) raftPort
    forkIO $ forever $ runAppT run appConfig

    -- Run API server.
    mapM_ (flip Warp.run (serve api (server appConfig))) apiPort
handleArgs _ = do
    putStrLn "Invalid arguments passed"
    putStrLn "Arguments are:"
    putStrLn "[config path] [server id]"
