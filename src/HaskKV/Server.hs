{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Reader
import Data.Binary
import Data.Conduit
import Data.Conduit.Network
import Data.Streaming.Network.Internal
import HaskKV.Types
import HaskKV.Utils
import System.Log.Logger

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

class (Monad m) => ServerM msg e m | m -> msg e where
    send      :: SID -> msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m (Either e msg)
    reset     :: e -> m ()
    serverIds :: m [SID]

data ServerState msg = ServerState
    { _messages         :: TBQueue msg
    , _outgoing         :: IM.IntMap (TBQueue msg)
    , _sid              :: Int
    , _electionTimer    :: Timer.Timer
    , _heartbeatTimer   :: Timer.Timer
    , _electionTimeout  :: Timeout
    , _heartbeatTimeout :: Timeout
    }

class HasServerState msg r | r -> msg where
    getServerState :: r -> ServerState msg

newServerState :: Capacity -> Timeout -> Timeout -> Int -> IO (ServerState msg)
newServerState backpressure electionTimeout heartbeatTimeout sid = do
    messages <- newTBQueueIO (unCapacity backpressure)
    electionTimer <- Timer.newIO
    heartbeatTimer <- Timer.newIO

    Timer.reset electionTimer electionTimeout
    Timer.reset heartbeatTimer heartbeatTimeout

    return ServerState
        { _messages         = messages
        , _outgoing         = IM.empty
        , _sid              = sid
        , _electionTimer    = electionTimer
        , _heartbeatTimer   = heartbeatTimer
        , _electionTimeout  = electionTimeout
        , _heartbeatTimeout = heartbeatTimeout
        }

retryTimeout :: Int
retryTimeout = 1000000

runServer :: (Binary msg, Show msg)
          => Int
          -> HostPreference
          -> IM.IntMap ClientSettings
          -> ServerState msg
          -> IO ()
runServer port host clients s = do
    forkIO $ runTCPServer (serverSettings port host) $ \appData ->
        runConduitRes
            $ appSource appData
           .| CL.mapFoldable (fmap thrd . decodeOrFail . BL.fromStrict)
           .| CL.iterM (liftIO . debugM "conduit" . ("Receiving: " ++) . show)
           .| sinkTBQueue (_messages s)

    forM_ (IM.assocs clients) $ \(i, settings) ->
        forM_ (IM.lookup i . _outgoing $ s) $ \bq ->
            forkIO $ connectClient settings bq
  where
    thrd t = let (_, _, a) = t in a

    connectClient settings bq = do
        let connect appData = putStrLn "Connected to server"
                           >> connectStream bq appData

        debugM "conduit" $ "Connecting to host "
                        ++ show (clientHost settings)
                        ++ " and port "
                        ++ show (clientPort settings)

        catch (runTCPClient settings connect) $ \(_ :: SomeException) -> do
            putStrLn "Error connecting to server, retrying"
            threadDelay retryTimeout
            connectClient settings bq

    connectStream bq appData
        = runConduit
        $ sourceTBQueue bq
       .| CL.iterM (liftIO . debugM "conduit" . ("Sending: " ++) . show)
       .| CL.map (B.concat . BL.toChunks . encode)
       .| appSink appData

data ServerEvent = ElectionTimeout
                 | HeartbeatTimeout
                 deriving (Show, Eq)

inject :: ServerEvent -> ServerState msg -> IO ()
inject HeartbeatTimeout s = Timer.reset (_heartbeatTimer s) 0
inject ElectionTimeout s  = Timer.reset (_electionTimer s) 0

newtype ServerT m a = ServerT { unServerT :: m a }
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

instance
    ( MonadIO m
    , MonadReader r m
    , HasServerState msg r
    ) => ServerM msg ServerEvent (ServerT m) where

    send i msg = liftIO . sendImpl i msg =<< asks getServerState
    broadcast msg = liftIO . broadcastImpl msg =<< asks getServerState
    recv = liftIO . recvImpl =<< asks getServerState
    reset e = liftIO . resetImpl e =<< asks getServerState
    serverIds = serverIdsImpl <$> asks getServerState

sendImpl :: SID -> msg -> ServerState msg -> IO ()
sendImpl (SID i) msg s = do
    let bq = IM.lookup i . _outgoing $ s
    mapM_ (atomically . flip writeTBQueue msg) bq

broadcastImpl :: msg -> ServerState msg -> IO ()
broadcastImpl msg ss = atomically
                     . mapM_ ((`writeTBQueue` msg) . snd)
                     . filter ((/= _sid ss) . fst)
                     . IM.assocs
                     $ _outgoing ss

recvImpl :: ServerState msg -> IO (Either ServerEvent msg)
recvImpl s = atomically $
    awaitElectionTimeout s
    `orElse` awaitHeartbeatTimeout s
    `orElse` (Right <$> readTBQueue (_messages s))
  where
    awaitElectionTimeout
        = fmap (const (Left ElectionTimeout))
        . Timer.await
        . _electionTimer
    awaitHeartbeatTimeout
        = fmap (const (Left HeartbeatTimeout))
        . Timer.await
        . _heartbeatTimer

resetImpl :: ServerEvent -> ServerState msg -> IO ()
resetImpl HeartbeatTimeout s =
    Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
resetImpl ElectionTimeout s = do
    timeout <- Timer.randTimeout $ _electionTimeout s
    Timer.reset (_electionTimer s) timeout

serverIdsImpl :: ServerState msg -> [SID]
serverIdsImpl = fmap SID . IM.keys . _outgoing
