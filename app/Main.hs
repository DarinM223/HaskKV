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

type MyKey    = Int
type MyValue  = StoreValue Int
type MyEntry  = LogEntry MyKey MyValue
type MyParams = Params RaftState RaftMessage MyKey MyValue MyEntry

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
    serverState <- configToServerState sid' config
    store <- newTVarIO emptyStore
    let params = Params
            { _store       = store
            , _serverState = serverState
            , _raftState   = createRaftState sid'
            } :: MyParams
        raftPort = configRaftPort sid' config
        apiPort  = configAPIPort sid' config
        settings = configToSettings sid' config

    -- Run Raft server and handler.
    mapM_ (\p -> runServer p "*" settings serverState) raftPort
    isLeader <- newTVarIO False
    forkIO $ raftLoop params isLeader

    -- Run API server.
    let context = RaftContext { _store       = store
                              , _isLeader    = isLeader
                              , _serverState = serverState
                              }
    mapM_ (\p -> Warp.run p (serve api (server context))) apiPort
  where
    raftLoop params isLeader = do
        (_, s') <- runRaftTParams run params
        case (_stateType $ _raftState params, _stateType s') of
            (Leader _, Leader _) -> return ()
            (Leader _, _)        -> atomically $ writeTVar isLeader False
            (_, Leader _)        -> atomically $ writeTVar isLeader True
            (_, _)               -> return ()
        raftLoop params { _raftState = s' } isLeader
handleArgs _ = do
    putStrLn "Invalid arguments passed"
    putStrLn "Arguments are:"
    putStrLn "[config path] [server id]"
