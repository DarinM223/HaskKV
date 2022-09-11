{-# LANGUAGE OverloadedLabels #-}

module HaskKV.Server.Instances where

import Control.Concurrent.STM (atomically, readTBQueue, writeTBQueue)
import Control.Applicative ((<|>))
import Data.Foldable (traverse_)
import HaskKV.Server.Types
import HaskKV.Types (SID (SID))
import Optics ((%), (^.), At (at))

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

inject :: ServerEvent -> ServerState msg -> IO ()
inject HeartbeatTimeout s = Timer.reset (s ^. #heartbeatTimer) 0
inject ElectionTimeout  s = Timer.reset (s ^. #electionTimer) 0

send :: SID -> msg -> ServerState msg -> IO ()
send (SID i) msg s =
  maybe (pure ()) (atomically . flip writeTBQueue msg) (s ^. #outgoing % at i)

broadcast :: msg -> ServerState msg -> IO ()
broadcast msg ss = atomically
                 . traverse_ ((`writeTBQueue` msg) . snd)
                 . filter ((/= ss ^. #sid) . SID . fst)
                 . IM.assocs
                 $ ss ^. #outgoing

recv :: ServerState msg -> IO (Either ServerEvent msg)
recv s = atomically $ awaitElectionTimeout s
                  <|> awaitHeartbeatTimeout s
                  <|> (Right <$> readTBQueue (s ^. #messages))
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . (^. #electionTimer)
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . (^. #heartbeatTimer)

reset :: ServerEvent -> ServerState msg -> IO ()
reset HeartbeatTimeout s =
  Timer.reset (s ^. #heartbeatTimer) (s ^. #heartbeatTimeout)
reset ElectionTimeout s = do
  timeout <- Timer.randTimeout $ s ^. #electionTimeout
  Timer.reset (s ^. #electionTimer) timeout

serverIds :: ServerState msg -> [SID]
serverIds = fmap SID . IM.keys . (^. #outgoing)
