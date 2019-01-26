module HaskKV.Config where

import Control.Concurrent.STM (newTBQueueIO)
import Control.Monad (foldM)
import Data.Conduit.Network (ClientSettings, clientSettings)
import Data.Foldable (find, foldl')
import Data.Maybe (catMaybes)
import HaskKV.Server.Types
import HaskKV.Server (mkServerState)
import HaskKV.Types
import Text.Read (readMaybe)

import qualified Data.ByteString.Char8 as C
import qualified Data.IntMap as IM

data ServerData = ServerData
  { _id          :: SID
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
parseConfig c = setData c . catMaybes . fmap attrsToServerData . splitInto 5
 where
  splitInto :: Int -> [String] -> [[String]]
  splitInto amount l
    | length l >= amount = take amount l : splitInto amount (drop amount l)
    | otherwise          = []

  setData c d = c { _serverData = d }

  attrsToServerData [id, host, raftPort, apiPort, dir] =
    ServerData
      <$> fmap SID (readMaybe id)
      <*> pure host
      <*> readMaybe raftPort
      <*> readMaybe apiPort
      <*> pure dir
  attrsToServerData _ = error "Invalid attributes"

readConfig :: Config -> FilePath -> IO Config
readConfig c = fmap (parseConfig c . lines) . readFile

configRaftPort :: SID -> Config -> Maybe Int
configRaftPort sid = fmap _raftPort . find ((== sid) . _id) . _serverData

configAPIPort :: SID -> Config -> Maybe Int
configAPIPort sid = fmap _apiPort . find ((== sid) . _id) . _serverData

configSnapshotDirectory :: SID -> Config -> Maybe FilePath
configSnapshotDirectory sid =
  fmap _snapshotDir . find ((== sid) . _id) . _serverData

configToSettings :: Config -> IM.IntMap ClientSettings
configToSettings = foldl' insert IM.empty . _serverData
 where
  insert settings ServerData { _id = sid, _raftPort = port, _host = host } =
    IM.insert (unSID sid) (clientSettings port $ C.pack host) settings

configToServerState :: SID -> Config -> IO (ServerState msg)
configToServerState sid config@Config { _backpressure = backpressure, _electionTimeout = eTimeout, _heartbeatTimeout = hTimeout }
  = do
    initServerState <- mkServerState backpressure eTimeout hTimeout sid
    outgoing' <- foldM (insert backpressure) (_outgoing initServerState)
               $ _serverData config
    return initServerState { _outgoing = outgoing' }
 where
  insert backpressure outgoing ServerData { _id = sid } = do
    bq <- newTBQueueIO $ fromIntegral $ unCapacity backpressure
    return $ IM.insert (unSID sid) bq outgoing
