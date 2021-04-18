{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server.Instances where

import Control.Concurrent.STM
import Control.Applicative ((<|>))
import Control.Monad.Reader
import Data.Foldable (traverse_)
import HaskKV.Server.Types
import HaskKV.Types
import Optics

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

inject :: ServerEvent -> ServerState msg -> IO ()
inject HeartbeatTimeout s = Timer.reset (s ^. #heartbeatTimer) 0
inject ElectionTimeout  s = Timer.reset (s ^. #electionTimer) 0

instance (MonadIO m, MonadReader r m, HasServerState msg r)
  => ServerM msg ServerEvent (ServerT m) where

  send i msg = gview serverStateL >>= liftIO . send' i msg
  broadcast msg = gview serverStateL >>= liftIO . broadcast' msg
  recv = gview serverStateL >>= liftIO . recv'
  reset e = gview serverStateL >>= liftIO . reset' e
  serverIds = serverIds' <$> gview serverStateL

send' :: SID -> msg -> ServerState msg -> IO ()
send' (SID i) msg s =
  traverse_ (atomically . flip writeTBQueue msg) (s ^. #outgoing % at i)

broadcast' :: msg -> ServerState msg -> IO ()
broadcast' msg ss = atomically
                  . traverse_ ((`writeTBQueue` msg) . snd)
                  . filter ((/= ss ^. #sid) . SID . fst)
                  . IM.assocs
                  $ ss ^. #outgoing

recv' :: ServerState msg -> IO (Either ServerEvent msg)
recv' s = atomically $ awaitElectionTimeout s
                   <|> awaitHeartbeatTimeout s
                   <|> (Right <$> readTBQueue (s ^. #messages))
 where
  awaitElectionTimeout =
    fmap (const (Left ElectionTimeout)) . Timer.await . (^. #electionTimer)
  awaitHeartbeatTimeout =
    fmap (const (Left HeartbeatTimeout)) . Timer.await . (^. #heartbeatTimer)

reset' :: ServerEvent -> ServerState msg -> IO ()
reset' HeartbeatTimeout s =
  Timer.reset (s ^. #heartbeatTimer) (s ^. #heartbeatTimeout)
reset' ElectionTimeout s = do
  timeout <- Timer.randTimeout $ s ^. #electionTimeout
  Timer.reset (s ^. #electionTimer) timeout

serverIds' :: ServerState msg -> [SID]
serverIds' = fmap SID . IM.keys . (^. #outgoing)
