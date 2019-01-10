module HaskKV.Server.Instances where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Monad.Reader
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

inject :: ServerEvent -> ServerState msg -> IO ()
inject HeartbeatTimeout s = Timer.reset (_heartbeatTimer s) 0
inject ElectionTimeout  s = Timer.reset (_electionTimer s) 0

type SSClass msg r m = (HasServerState msg r, MonadReader r m, MonadIO m)

getSS :: SSClass msg r m => (ServerState msg -> IO a) -> m a
getSS m = asks getServerState >>= liftIO . m

serverMIO :: SSClass msg r m => ServerM msg ServerEvent m
serverMIO = ServerM
  { send      = send'
  , broadcast = broadcast'
  , recv      = recv'
  , reset     = reset'
  , serverIds = serverIds' <$> asks getServerState
  }

send' :: SSClass msg r m => SID -> msg -> m ()
send' (SID i) msg = getSS $ \state -> do
  let bq = IM.lookup i . _outgoing $ state
  mapM_ (atomically . flip writeTBQueue msg) bq

broadcast' :: SSClass msg r m => msg -> m ()
broadcast' msg = getSS $ \ss ->
  atomically
    . mapM_ ((`writeTBQueue` msg) . snd)
    . filter ((/= _sid ss) . SID . fst)
    . IM.assocs
    $ _outgoing ss

recv' :: SSClass msg r m => m (Either ServerEvent msg)
recv' = getSS $ \s ->
  atomically
    $   awaitElectionTimeout s
    <|> awaitHeartbeatTimeout s
    <|> (Right <$> readTBQueue (_messages s))
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . _electionTimer
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . _heartbeatTimer

reset' :: SSClass msg r m => ServerEvent -> m ()
reset' HeartbeatTimeout = getSS $ \s ->
  Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
resetImpl ElectionTimeout s = getSS $ \s -> do
  timeout <- Timer.randTimeout $ _electionTimeout s
  Timer.reset (_electionTimer s) timeout

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . _outgoing
