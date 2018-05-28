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
import HaskKV.Utils
import System.Log.Logger

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ
import qualified HaskKV.Timer as Timer

class (Monad m) => ServerM msg e m | m -> msg e where
    send      :: Int -> msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m (Either e msg)
    reset     :: e -> m ()
    inject    :: e -> m ()
    serverIds :: m [Int]

newtype Capacity = Capacity { unCapacity :: Int } deriving (Show, Eq)

data ServerState msg = ServerState
    { _messages         :: RQ.RollingQueue msg
    , _outgoing         :: IM.IntMap (RQ.RollingQueue msg)
    , _electionTimer    :: Timer.Timer
    , _heartbeatTimer   :: Timer.Timer
    , _electionTimeout  :: Timer.Timeout
    , _heartbeatTimeout :: Timer.Timeout
    }

createServerState :: Capacity
                  -> Timer.Timeout
                  -> Timer.Timeout
                  -> IO (ServerState msg)
createServerState backpressure electionTimeout heartbeatTimeout = do
    messages <- RQ.newIO (unCapacity backpressure)
    electionTimer <- Timer.newIO
    heartbeatTimer <- Timer.newIO

    Timer.reset electionTimer electionTimeout
    Timer.reset heartbeatTimer heartbeatTimeout

    return ServerState
        { _messages         = messages
        , _outgoing         = IM.empty
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
           .| sinkRollingQueue (_messages s)

    forM_ (IM.assocs clients) $ \(i, settings) -> do
        forM_ (IM.lookup i . _outgoing $ s) $ \rq -> do
            forkIO $ connectClient settings rq
  where
    thrd t = let (_, _, a) = t in a

    connectClient settings rq = do
        let connect appData = putStrLn "Connected to server"
                           >> connectStream rq appData

        debugM "conduit" $ "Connecting to host "
                        ++ show (clientHost settings)
                        ++ " and port "
                        ++ show (clientPort settings)

        catch (runTCPClient settings connect) $ \(_ :: SomeException) -> do
            putStrLn "Error connecting to server, retrying"
            threadDelay retryTimeout
            connectClient settings rq

    connectStream rq appData
        = runConduit
        $ sourceRollingQueue rq
       .| CL.iterM (liftIO . debugM "conduit" . ("Sending: " ++) . show)
       .| CL.map (B.concat . BL.toChunks . encode)
       .| appSink appData

data ServerEvent = ElectionTimeout
                 | HeartbeatTimeout
                 deriving (Show, Eq)

-- ServerM instance implementations

sendImpl i msg s = do
    let rq = IM.lookup i . _outgoing $ s
    mapM_ (liftIO . atomically . flip RQ.write msg) rq

broadcastImpl msg
    = liftIO
    . atomically
    . mapM_ (`RQ.write` msg)
    . IM.elems
    . _outgoing

recvImpl s =
    liftIO . atomically $
        awaitElectionTimeout s
        `orElse` awaitHeartbeatTimeout s
        `orElse` (Right . fst <$> RQ.read (_messages s))
  where
    awaitElectionTimeout
        = fmap (const (Left ElectionTimeout))
        . Timer.await
        . _electionTimer
    awaitHeartbeatTimeout
        = fmap (const (Left HeartbeatTimeout))
        . Timer.await
        . _heartbeatTimer

injectImpl HeartbeatTimeout s =
    liftIO $ Timer.reset (_heartbeatTimer s) (Timer.Timeout 0)
injectImpl ElectionTimeout s =
    liftIO $ Timer.reset (_electionTimer s) (Timer.Timeout 0)

resetImpl HeartbeatTimeout s =
    liftIO $ Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
resetImpl ElectionTimeout s = do
    timeout <- liftIO $ Timer.randTimeout $ _electionTimeout s
    liftIO $ Timer.reset (_electionTimer s) timeout

serverIdsImpl = IM.keys . _outgoing
