module HaskKV.Server.Types where

import Control.Concurrent.STM
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
