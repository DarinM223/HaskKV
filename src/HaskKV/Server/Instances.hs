{-# LANGUAGE UndecidableInstances #-}

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
broadcastImpl msg ss =
  atomically
    . mapM_ ((`writeTBQueue` msg) . snd)
    . filter ((/= _sid ss) . SID . fst)
    . IM.assocs
    $ _outgoing ss

recvImpl :: ServerState msg -> IO (Either ServerEvent msg)
recvImpl s =
  atomically
    $   awaitElectionTimeout s
    <|> awaitHeartbeatTimeout s
    <|> Right
    <$> readTBQueue (_messages s)
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . _electionTimer
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . _heartbeatTimer

resetImpl :: ServerEvent -> ServerState msg -> IO ()
resetImpl HeartbeatTimeout s =
  Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
resetImpl ElectionTimeout s = do
  timeout <- Timer.randTimeout $ _electionTimeout s
  Timer.reset (_electionTimer s) timeout

serverIdsImpl :: ServerState msg -> [SID]
serverIdsImpl = fmap SID . IM.keys . _outgoing
