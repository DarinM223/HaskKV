module Main where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import HaskKV
import Servant.Server (serve)
import System.Environment (getArgs)
import System.Log.Logger
import System.Log.Handler.Simple
import System.Log.Handler (setFormatter)
import System.Log.Formatter

import qualified Network.Wai.Handler.Warp as Warp

main :: IO ()
main = getArgs >>= handleArgs

type MyKey     = Int
type MyValue   = StoreValue Int
type MyEntry   = LogEntry MyKey MyValue
type MyMessage = RaftMessage MyEntry
type MyConfig  = AppConfig MyMessage MyKey MyValue MyEntry

handleArgs :: [String] -> IO ()
handleArgs (path:sid:_) = do
    updateGlobalLogger sid (setLevel DEBUG)
    updateGlobalLogger "conduit" (setLevel DEBUG)
    h <- fileHandler (sid ++ ".log") DEBUG >>= \lh -> return $
        setFormatter lh (simpleLogFormatter "[$time : $loggername : $prio] $msg")
    updateGlobalLogger sid (addHandler h)
    updateGlobalLogger "conduit" (addHandler h)

    let sid'       = read sid :: Int
        initConfig = Config
            { _backpressure     = Capacity 100
            , _electionTimeout  = Timeout (2000000 :: Int)
            , _heartbeatTimeout = Timeout (1000000 :: Int)
            , _serverData       = []
            }
    config <- readConfig initConfig path
    isLeader <- newTVarIO False
    serverState <- configToServerState config
    store <- newTVarIO emptyStore
    let raftState = createRaftState sid'
        appConfig = AppConfig
            { _store       = store
            , _serverState = serverState
            , _isLeader    = isLeader
            } :: MyConfig
        raftPort = configRaftPort sid' config
        apiPort  = configAPIPort sid' config
        settings = configToSettings config

    -- Run Raft server and handler.
    mapM_ (\p -> runServer p "*" settings serverState) raftPort
    forkIO $ raftLoop appConfig raftState isLeader

    -- Run API server.
    mapM_ (\p -> Warp.run p (serve api (server appConfig))) apiPort
  where
    raftLoop appConfig raftState isLeader = do
        (_, s') <- runAppT run appConfig raftState
        case (_stateType raftState, _stateType s') of
            (Leader _, Leader _) -> return ()
            (Leader _, _)        -> atomically $ writeTVar isLeader False
            (_, Leader _)        -> atomically $ writeTVar isLeader True
            (_, _)               -> return ()
        raftLoop appConfig s' isLeader
handleArgs _ = do
    putStrLn "Invalid arguments passed"
    putStrLn "Arguments are:"
    putStrLn "[config path] [server id]"
