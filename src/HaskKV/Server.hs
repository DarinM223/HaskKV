module HaskKV.Server
  ( mkServerState
  , mkServerM
  , runServer
  ) where

import Control.Applicative ((<|>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Reader
import Data.Binary
import Data.Conduit
import Data.Conduit.Network
import Data.Streaming.Network.Internal
import HaskKV.Server.Types
import HaskKV.Types
import HaskKV.Utils
import System.Log.Logger

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

mkServerState :: Capacity -> Timeout -> Timeout -> SID -> IO (ServerState msg)
mkServerState backpressure electionTimeout heartbeatTimeout sid = do
  messages       <- newTBQueueIO $ fromIntegral $ unCapacity backpressure
  electionTimer  <- Timer.newIO
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

mkServerM :: MonadIO m => ServerState msg -> ServerM msg ServerEvent m
mkServerM state = ServerM
  { send      = send' state
  , broadcast = broadcast' state
  , recv      = recv' state
  , reset     = reset' state
  , inject    = inject' state
  , serverIds = pure $ serverIds' state
  }

retryTimeout :: Int
retryTimeout = 1000000

runServer
  :: (Binary msg, Show msg)
  => Int
  -> HostPreference
  -> IM.IntMap ClientSettings
  -> ServerState msg
  -> IO ()
runServer port host clients s = do
  forkIO $ runTCPServer (serverSettings port host) $ \appData ->
    runConduitRes
      $  appSource appData
      .| CL.mapFoldable (fmap thrd . decodeOrFail . BL.fromStrict)
      .| CL.iterM (liftIO . debugM "conduit" . ("Receiving: " ++) . show)
      .| sinkTBQueue (_messages s)

  forM_ (IM.assocs clients) $ \(i, settings) ->
    forM_ (IM.lookup i . _outgoing $ s)
      $ \bq -> forkIO $ connectClient settings bq
 where
  thrd t = let (_, _, a) = t in a

  connectClient settings bq = do
    let
      connect appData =
        putStrLn "Connected to server" >> connectStream bq appData

    debugM "conduit"
      $  "Connecting to host "
      ++ show (clientHost settings)
      ++ " and port "
      ++ show (clientPort settings)

    catch (runTCPClient settings connect) $ \(_ :: SomeException) -> do
      putStrLn "Error connecting to server, retrying"
      threadDelay retryTimeout
      connectClient settings bq

  connectStream bq appData =
    runConduit
      $  sourceTBQueue bq
      .| CL.iterM (liftIO . debugM "conduit" . ("Sending: " ++) . show)
      .| CL.map (B.concat . BL.toChunks . encode)
      .| appSink appData

send' :: MonadIO m => ServerState msg -> SID -> msg -> m ()
send' state (SID i) msg = liftIO $ mapM_ (atomically . flip writeTBQueue msg) bq
 where
  bq = IM.lookup i . _outgoing $ state

broadcast' :: MonadIO m => ServerState msg -> msg -> m ()
broadcast' ss msg =
  liftIO . atomically
    . mapM_ ((`writeTBQueue` msg) . snd)
    . filter ((/= _sid ss) . SID . fst)
    . IM.assocs
    $ _outgoing ss

recv' :: MonadIO m => ServerState msg -> m (Either ServerEvent msg)
recv' s =
  liftIO . atomically
    $   awaitElectionTimeout s
    <|> awaitHeartbeatTimeout s
    <|> (Right <$> readTBQueue (_messages s))
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . _electionTimer
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . _heartbeatTimer

reset' :: MonadIO m => ServerState msg -> ServerEvent -> m ()
reset' s = liftIO . \case
  HeartbeatTimeout -> Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
  ElectionTimeout  -> do
    timeout <- Timer.randTimeout $ _electionTimeout s
    Timer.reset (_electionTimer s) timeout

inject' :: MonadIO m => ServerState msg -> ServerEvent -> m ()
inject' s = \case
  HeartbeatTimeout -> Timer.reset (_heartbeatTimer s) 0
  ElectionTimeout  -> Timer.reset (_electionTimer s) 0

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . _outgoing
