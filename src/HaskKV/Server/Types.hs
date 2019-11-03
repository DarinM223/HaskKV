{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Server.Types where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Types
import Optics

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

class (Monad m) => ServerM msg e m | m -> msg e where
  send      :: SID -> msg -> m ()
  broadcast :: msg -> m ()
  recv      :: m (Either e msg)
  reset     :: e -> m ()
  serverIds :: m [SID]

data ServerEvent = ElectionTimeout
                 | HeartbeatTimeout
                 deriving (Show, Eq)

data ServerState msg = ServerState
  { serverStateMessages         :: TBQueue msg
  , serverStateOutgoing         :: IM.IntMap (TBQueue msg)
  , serverStateSid              :: SID
  , serverStateElectionTimer    :: Timer.Timer
  , serverStateHeartbeatTimer   :: Timer.Timer
  , serverStateElectionTimeout  :: Timeout
  , serverStateHeartbeatTimeout :: Timeout
  }
makeFieldLabels ''ServerState

class HasServerState msg r | r -> msg where
  serverStateL :: Lens' r (ServerState msg)

newtype ServerT m a = ServerT { unServerT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

newServerState :: Capacity -> Timeout -> Timeout -> SID -> IO (ServerState msg)
newServerState backpressure electionTimeout heartbeatTimeout sid = do
  messages       <- newTBQueueIO $ fromIntegral $ unCapacity backpressure
  electionTimer  <- Timer.newIO
  heartbeatTimer <- Timer.newIO

  Timer.reset electionTimer electionTimeout
  Timer.reset heartbeatTimer heartbeatTimeout

  return ServerState
    { serverStateMessages         = messages
    , serverStateOutgoing         = IM.empty
    , serverStateSid              = sid
    , serverStateElectionTimer    = electionTimer
    , serverStateHeartbeatTimer   = heartbeatTimer
    , serverStateElectionTimeout  = electionTimeout
    , serverStateHeartbeatTimeout = heartbeatTimeout
    }
