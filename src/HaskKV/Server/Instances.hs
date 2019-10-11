{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server.Instances where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Lens
import Control.Monad.Reader
import HaskKV.Server.Types
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

inject :: ServerEvent -> ServerState msg -> IO ()
inject HeartbeatTimeout s = Timer.reset (_heartbeatTimer s) 0
inject ElectionTimeout  s = Timer.reset (_electionTimer s) 0

instance (MonadIO m, MonadReader r m, HasServerState msg r)
  => ServerM msg ServerEvent (ServerT m) where

  send i msg = view serverStateL >>= liftIO . send' i msg
  broadcast msg = view serverStateL >>= liftIO . broadcast' msg
  recv = view serverStateL >>= liftIO . recv'
  reset e = view serverStateL >>= liftIO . reset' e
  serverIds = serverIds' <$> view serverStateL

send' :: SID -> msg -> ServerState msg -> IO ()
send' (SID i) msg s = do
  let bq = IM.lookup i . _outgoing $ s
  mapM_ (atomically . flip writeTBQueue msg) bq

broadcast' :: msg -> ServerState msg -> IO ()
broadcast' msg ss = atomically
                  . mapM_ ((`writeTBQueue` msg) . snd)
                  . filter ((/= _sid ss) . SID . fst)
                  . IM.assocs
                  $ _outgoing ss

recv' :: ServerState msg -> IO (Either ServerEvent msg)
recv' s = atomically $ awaitElectionTimeout s
                   <|> awaitHeartbeatTimeout s
                   <|> (Right <$> readTBQueue (_messages s))
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . _electionTimer
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . _heartbeatTimer

reset' :: ServerEvent -> ServerState msg -> IO ()
reset' HeartbeatTimeout s =
  Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
reset' ElectionTimeout s = do
  timeout <- Timer.randTimeout $ _electionTimeout s
  Timer.reset (_electionTimer s) timeout

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . _outgoing
