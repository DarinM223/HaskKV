module HaskKV.Config where

import Control.Concurrent.STM (newTBQueueIO)
import Control.Monad (foldM)
import Data.Conduit.Network (ClientSettings, clientSettings)
import Data.Foldable (find, foldl')
import Data.Maybe (mapMaybe)
import GHC.Generics
import HaskKV.Server.Types
import HaskKV.Types
import Optics
import Text.Read (readMaybe)

import qualified Data.ByteString.Char8 as C
import qualified Data.IntMap as IM

data ServerData = ServerData
  { id          :: SID
  , host        :: String
  , raftPort    :: Int
  , apiPort     :: Int
  , snapshotDir :: FilePath
  } deriving (Show, Eq, Generic)

data Config = Config
  { backpressure     :: Capacity
  , electionTimeout  :: Timeout
  , heartbeatTimeout :: Timeout
  , serverData       :: [ServerData]
  } deriving (Show, Eq, Generic)

parseConfig :: Config -> [String] -> Config
parseConfig c = setData c . mapMaybe attrsToServerData . splitInto 5
 where
  splitInto :: Int -> [String] -> [[String]]
  splitInto amount l
    | length l >= amount = take amount l : splitInto amount (drop amount l)
    | otherwise          = []

  setData c d = c & #serverData .~ d

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
configRaftPort sid =
  fmap (^. #raftPort) . find ((== sid) . (^. #id)) . (^. #serverData)

configAPIPort :: SID -> Config -> Maybe Int
configAPIPort sid =
  fmap (^. #apiPort) . find ((== sid) . (^. #id)) . (^. #serverData)

configSnapshotDirectory :: SID -> Config -> Maybe FilePath
configSnapshotDirectory sid =
  fmap (^. #snapshotDir) . find ((== sid) . (^. #id)) . (^. #serverData)

configToSettings :: Config -> IM.IntMap ClientSettings
configToSettings = foldl' insert IM.empty . (^. #serverData)
 where
  insert settings d = IM.insert
    (unSID (d ^. #id))
    (clientSettings (d ^. #raftPort) $ C.pack (d ^. #host))
    settings

configToServerState :: SID -> Config -> IO (ServerState msg)
configToServerState sid c = do
  let backpressure = c ^. #backpressure
  ss <- newServerState
    backpressure
    (c ^. #electionTimeout)
    (c ^. #heartbeatTimeout)
    sid
  outgoing' <- foldM (insert backpressure) (ss ^. #outgoing) $ c ^. #serverData
  return $ ss & #outgoing .~ outgoing'
 where
  insert backpressure outgoing d = do
    bq <- newTBQueueIO $ fromIntegral $ unCapacity backpressure
    return $ IM.insert (unSID (d ^. #id)) bq outgoing
