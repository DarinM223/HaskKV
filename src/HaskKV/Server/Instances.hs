module HaskKV.Server.Instances (mkServerM) where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Monad.Reader
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

mkServerM :: MonadIO m => ServerState msg -> ServerM msg ServerEvent m
mkServerM state = ServerM
  { send      = send' state
  , broadcast = broadcast' state
  , recv      = recv' state
  , reset     = reset' state
  , inject    = inject' state
  , serverIds = pure $ serverIds' state
  }

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
