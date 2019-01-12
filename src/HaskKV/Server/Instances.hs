{-# LANGUAGE AllowAmbiguousTypes #-}

module HaskKV.Server.Instances where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Monad.Reader
import Data.Generics.Product.Typed
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

type SSClass msg r m = (HasType (ServerState msg) r, MonadReader r m, MonadIO m)

getSS :: forall msg r m a. SSClass msg r m => (ServerState msg -> IO a) -> m a
getSS m = asks (getTyped @(ServerState msg)) >>= liftIO . m

mkServerM :: forall msg r m. SSClass msg r m => ServerM msg ServerEvent m
mkServerM = ServerM
  { send      = send'
  , broadcast = broadcast'
  , recv      = recv'
  , reset     = reset' @msg
  , inject    = inject' @msg
  , serverIds = serverIds' <$> asks (getTyped @(ServerState msg))
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

reset' :: forall msg r m. SSClass msg r m => ServerEvent -> m ()
reset' HeartbeatTimeout = getSS @msg $ \s ->
  Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
reset' ElectionTimeout = getSS @msg $ \s -> do
  timeout <- Timer.randTimeout $ _electionTimeout s
  Timer.reset (_electionTimer s) timeout

inject' :: forall msg r m. SSClass msg r m => ServerEvent -> m ()
inject' HeartbeatTimeout = getSS @msg $ \s -> Timer.reset (_heartbeatTimer s) 0
inject' ElectionTimeout  = getSS @msg $ \s -> Timer.reset (_electionTimer s) 0

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . _outgoing
