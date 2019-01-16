module HaskKV.Server.Types where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Types

import qualified Data.IntMap as IM
import qualified HaskKV.Timer as Timer

data ServerM msg e m = ServerM
  { send      :: SID -> msg -> m ()
  , broadcast :: msg -> m ()
  , recv      :: m (Either e msg)
  , reset     :: e -> m ()
  , inject    :: e -> m ()
  , serverIds :: m [SID]
  }
class HasServerM msg e m effs | effs -> msg e m where
  getServerM :: effs -> ServerM msg e m

data ServerEvent = ElectionTimeout
                 | HeartbeatTimeout
                 deriving (Show, Eq)

data ServerState msg = ServerState
  { _messages         :: TBQueue msg
  , _outgoing         :: IM.IntMap (TBQueue msg)
  , _sid              :: SID
  , _electionTimer    :: Timer.Timer
  , _heartbeatTimer   :: Timer.Timer
  , _electionTimeout  :: Timeout
  , _heartbeatTimeout :: Timeout
  }
class HasServerState msg cfg | cfg -> msg where
  getServerState :: cfg -> ServerState msg

newServerState :: Capacity -> Timeout -> Timeout -> SID -> IO (ServerState msg)
newServerState backpressure electionTimeout heartbeatTimeout sid = do
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
