{-# LANGUAGE FunctionalDependencies #-}
module HaskKV.Server.Types where

import Control.Concurrent.STM (TBQueue, newTBQueueIO)
import Control.Monad.Reader (MonadIO, MonadReader)
import GHC.Generics (Generic)
import HaskKV.Types (Capacity (unCapacity), SID, Timeout)
import Optics (Lens')

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
  { messages         :: TBQueue msg
  , outgoing         :: IM.IntMap (TBQueue msg)
  , sid              :: SID
  , electionTimer    :: Timer.Timer
  , heartbeatTimer   :: Timer.Timer
  , electionTimeout  :: Timeout
  , heartbeatTimeout :: Timeout
  } deriving Generic

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
    { messages         = messages
    , outgoing         = IM.empty
    , sid              = sid
    , electionTimer    = electionTimer
    , heartbeatTimer   = heartbeatTimer
    , electionTimeout  = electionTimeout
    , heartbeatTimeout = heartbeatTimeout
    }
