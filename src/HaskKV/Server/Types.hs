module HaskKV.Server.Types where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Types

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
  { _messages         :: TBQueue msg
  , _outgoing         :: IM.IntMap (TBQueue msg)
  , _sid              :: SID
  , _electionTimer    :: Timer.Timer
  , _heartbeatTimer   :: Timer.Timer
  , _electionTimeout  :: Timeout
  , _heartbeatTimeout :: Timeout
  }

class HasServerState msg r | r -> msg where
  getServerState :: r -> ServerState msg

newtype ServerT m a = ServerT { unServerT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)

newServerState :: Capacity -> Timeout -> Timeout -> SID -> IO (ServerState msg)
newServerState backpressure electionTimeout heartbeatTimeout sid = do
  messages       <- newTBQueueIO (unCapacity backpressure)
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
