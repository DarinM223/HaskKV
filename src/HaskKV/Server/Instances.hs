module HaskKV.Server.Instances (mkServerM) where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Monad.Reader
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

type HasSS msg cfg m = (HasServerState msg cfg, MonadIO m)

mkServerM :: HasSS msg cfg m => cfg -> ServerM msg ServerEvent m
mkServerM cfg = ServerM
  { send      = send' cfg
  , broadcast = broadcast' cfg
  , recv      = recv' cfg
  , reset     = reset' cfg
  , inject    = inject' cfg
  , serverIds = pure $ serverIds' $ getServerState cfg
  }

send' :: HasSS msg cfg m => cfg -> SID -> msg -> m ()
send' cfg (SID i) msg = liftIO $ mapM_ (atomically . flip writeTBQueue msg) bq
 where
  state = getServerState cfg
  bq = IM.lookup i . _outgoing $ state

broadcast' :: HasSS msg cfg m => cfg -> msg -> m ()
broadcast' cfg msg =
  liftIO . atomically
    . mapM_ ((`writeTBQueue` msg) . snd)
    . filter ((/= _sid ss) . SID . fst)
    . IM.assocs
    $ _outgoing ss
 where ss = getServerState cfg

recv' :: HasSS msg cfg m => cfg -> m (Either ServerEvent msg)
recv' cfg =
  liftIO . atomically
    $   awaitElectionTimeout s
    <|> awaitHeartbeatTimeout s
    <|> (Right <$> readTBQueue (_messages s))
 where
  s = getServerState cfg
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . _electionTimer
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . _heartbeatTimer

reset' :: HasSS msg cfg m => cfg -> ServerEvent -> m ()
reset' cfg = liftIO . \case
  HeartbeatTimeout -> Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
  ElectionTimeout  -> do
    timeout <- Timer.randTimeout $ _electionTimeout s
    Timer.reset (_electionTimer s) timeout
 where s = getServerState cfg

inject' :: HasSS msg cfg m => cfg -> ServerEvent -> m ()
inject' cfg = \case
  HeartbeatTimeout -> Timer.reset (_heartbeatTimer s) 0
  ElectionTimeout  -> Timer.reset (_electionTimer s) 0
 where s = getServerState cfg

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . _outgoing
