module HaskKV.Config where

import Data.Conduit.Network
import Data.List
import Data.Maybe
import Control.Monad
import HaskKV.Server
import HaskKV.Timer
import Text.Read

import qualified Data.ByteString.Char8 as C
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ

data ServerData = ServerData
    { _id   :: Int
    , _host :: String
    , _port :: Int
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
              . splitIntoThrees
  where
    splitIntoThrees (a:b:c:xs) = [a, b, c]:splitIntoThrees xs
    splitIntoThrees _          = []

    setData c d = c { _serverData = d }

    attrsToServerData [id, host, port] = ServerData
                                     <$> readMaybe id
                                     <*> pure host
                                     <*> readMaybe port
    attrsToServerData _ = error "Invalid attributes"

readConfig :: Config -> FilePath -> IO Config
readConfig c = fmap (parseConfig c . lines) . readFile

configServerPort :: Int -> Config -> Maybe Int
configServerPort sid = fmap _port . find ((== sid) . _id) . _serverData

configToSettings :: Int -> Config -> IM.IntMap ClientSettings
configToSettings sid = foldl' insert IM.empty
                     . filter ((/= sid) . _id)
                     . _serverData
  where
    insert settings ServerData{ _id = sid
                              , _port = port
                              , _host = host
                              } =
        IM.insert sid (clientSettings port $ C.pack host) settings

configToServerState :: Int -> Config -> IO (ServerState msg)
configToServerState sid config@Config{ _backpressure     = backpressure
                                     , _electionTimeout  = eTimeout
                                     , _heartbeatTimeout = hTimeout
                                     } = do
    initServerState <- createServerState backpressure eTimeout hTimeout
    outgoing' <- foldM (insert backpressure) (_outgoing initServerState)
               . filter ((/= sid) . _id)
               . _serverData
               $ config
    return initServerState { _outgoing = outgoing' }
  where
    insert backpressure outgoing ServerData{_id = sid} = do
        rq <- RQ.newIO (unCapacity backpressure)
        return $ IM.insert sid rq outgoing
