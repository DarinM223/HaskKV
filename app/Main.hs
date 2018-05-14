module Main where

import Control.Concurrent.STM
import HaskKV
import System.Environment (getArgs)
import System.Log.Logger
import System.Log.Handler.Simple
import System.Log.Handler (setFormatter)
import System.Log.Formatter

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
        port     = configServerPort sid' config
        settings = configToSettings sid' config
    mapM_ (\p -> runServer p "*" settings serverState) port
    runRaftTParams run params
handleArgs _ = do
    putStrLn "Invalid arguments passed"
    putStrLn "Arguments are:"
    putStrLn "[config path] [server id]"
