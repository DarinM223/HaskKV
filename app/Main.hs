module Main where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Binary
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
    serverState <- configToServerState config
    let raftState   = newRaftState sid'
        raftPort    = configRaftPort sid' config
        apiPort     = configAPIPort sid' config
        settings    = configToSettings config
        snapshotDir = configSnapshotDirectory sid' config
    appConfig <- newAppConfig snapshotDir serverState :: IO MyConfig

    -- Add init entries to log and store.
    initEntries <- readEntries $ sid ++ ".init"
    unless (null initEntries) $
        debugM "conduit" $ "Loading init entries: " ++ show initEntries
    flip runAppTConfig appConfig $ do
        storeEntries initEntries
        mapM_ applyEntry initEntries

        info <- snapshotInfo
        snapshot <- maybe (pure Nothing) (readSnapshot . fst) info
        case (info, snapshot) of
            (Just (index, term), Just snap) -> loadSnapshot index term snap
            _                               -> return ()

    -- Run Raft server and handler.
    mapM_ (\p -> runServer p "*" settings serverState) raftPort
    forkIO $ raftLoop appConfig raftState

    -- Run API server.
    mapM_ (\p -> Warp.run p (serve api (server appConfig))) apiPort
  where
    raftLoop appConfig raftState = do
        (_, s') <- runAppT run appConfig raftState
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

readEntries :: (Binary k, Binary v)
            => FilePath
            -> IO [LogEntry k v]
readEntries = handle (\(_ :: SomeException) -> return [])
            . fmap (either (const []) id)
            . decodeFileOrFail

writeEntries :: (Binary k, Binary v)
             => FilePath
             -> [LogEntry k v]
             -> IO ()
writeEntries = encodeFile
