module HaskKV.Config where

import Control.Concurrent.STM
import Control.Monad
import Data.Conduit.Network
import Data.List
import Data.Maybe
import HaskKV.Server
import HaskKV.Timer
import Text.Read

import qualified Data.ByteString.Char8 as C
import qualified Data.IntMap as IM

data ServerData = ServerData
    { _id          :: Int
    , _host        :: String
    , _raftPort    :: Int
    , _apiPort     :: Int
    , _snapshotDir :: FilePath
    } deriving (Show, Eq)

data Config = Config
    { _backpressure     :: Capacity
    , _electionTimeout  :: Timeout
    , _heartbeatTimeout :: Timeout
    , _serverData       :: [ServerData]
    } deriving (Show, Eq)

parseConfig :: Config -> [String] -> Config
parseConfig c = setData c
              . catMaybes
              . fmap attrsToServerData
              . splitInto 5
  where
    splitInto :: Int -> [String] -> [[String]]
    splitInto amount l
        | length l >= amount = take amount l:splitInto amount (drop amount l)
        | otherwise          = []

    setData c d = c { _serverData = d }

    attrsToServerData [id, host, raftPort, apiPort, dir] = ServerData
                                                       <$> readMaybe id
                                                       <*> pure host
                                                       <*> readMaybe raftPort
                                                       <*> readMaybe apiPort
                                                       <*> pure dir
    attrsToServerData _ = error "Invalid attributes"

readConfig :: Config -> FilePath -> IO Config
readConfig c = fmap (parseConfig c . lines) . readFile

configRaftPort :: Int -> Config -> Maybe Int
configRaftPort sid = fmap _raftPort . find ((== sid) . _id) . _serverData

configAPIPort :: Int -> Config -> Maybe Int
configAPIPort sid = fmap _apiPort . find ((== sid) . _id) . _serverData

configSnapshotDirectory :: Int ->  Config -> Maybe FilePath
configSnapshotDirectory sid = fmap _snapshotDir
                            . find ((== sid) . _id)
                            . _serverData

configToSettings :: Config -> IM.IntMap ClientSettings
configToSettings = foldl' insert IM.empty . _serverData
  where
    insert settings ServerData{ _id       = sid
                              , _raftPort = port
                              , _host     = host
                              } =
        IM.insert sid (clientSettings port $ C.pack host) settings

configToServerState :: Int -> Config -> IO (ServerState msg)
configToServerState sid config@Config{ _backpressure     = backpressure
                                     , _electionTimeout  = eTimeout
                                     , _heartbeatTimeout = hTimeout
                                     } = do
    initServerState <- newServerState backpressure eTimeout hTimeout sid
    outgoing' <- foldM (insert backpressure) (_outgoing initServerState)
               . _serverData
               $ config
    return initServerState { _outgoing = outgoing' }
  where
    insert backpressure outgoing ServerData{_id = sid} = do
        bq <- newTBQueueIO (unCapacity backpressure)
        return $ IM.insert sid bq outgoing
