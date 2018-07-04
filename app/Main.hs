module Main where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Trans
import Control.Monad.Trans.Maybe
import HaskKV
import HaskKV.Store
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
    h <- fileHandler (sid ++ ".out") DEBUG >>= \lh -> return $
        setFormatter lh (simpleLogFormatter "[$time : $loggername : $prio] $msg")
    updateGlobalLogger sid (addHandler h)
    updateGlobalLogger "conduit" (addHandler h)

    let sid'       = SID $ read sid
        initConfig = Config
            { _backpressure     = Capacity 100
            , _electionTimeout  = 2000000
            , _heartbeatTimeout = 1000000
            , _serverData       = []
            }
    config <- readConfig initConfig path
    serverState <- configToServerState sid' config
    persistentState <- loadBinary persistentStateFilename sid'
    let raftState   = newRaftState sid' persistentState
        raftPort    = configRaftPort sid' config
        apiPort     = configAPIPort sid' config
        settings    = configToSettings config
        snapshotDir = configSnapshotDirectory sid' config
    initLog <- loadBinary logFilename sid'
    case initLog of
        Just log -> debugM "conduit" $ "Loading log: " ++ show log
        _        -> return ()
    appConfig <- newAppConfig snapshotDir initLog serverState :: IO MyConfig

    flip runAppTConfig appConfig $ do
        runMaybeT $ do
            (index, term, _) <- lift snapshotInfo >>= MaybeT . pure
            snapshot <- lift (readSnapshot index) >>= MaybeT . pure
            lift $ loadSnapshot index term snapshot

    -- Run Raft server and handler.
    mapM_ (\p -> runServer p "*" settings serverState) raftPort
    forkIO $ raftLoop appConfig raftState

    -- Run API server.
    mapM_ (\p -> Warp.run p (serve api (server appConfig))) apiPort
  where
    raftLoop appConfig raftState = do
        (_, s') <- runAppT run appConfig raftState
        persistBinary persistentStateFilename
                      (_serverID s')
                      (newPersistentState s')
        case (_stateType raftState, _stateType s') of
            (Leader _, Leader _) -> return ()
            (Leader _, _) ->
                atomically $ writeTVar (_isLeader appConfig) False
            (_, Leader _) ->
                atomically $ writeTVar (_isLeader appConfig) True
            (_, _) -> return ()
        raftLoop appConfig s'
handleArgs _ = do
    putStrLn "Invalid arguments passed"
    putStrLn "Arguments are:"
    putStrLn "[config path] [server id]"
