module HaskKV.Timer where

-- Implementation taken from: https://github.com/NicolasT/kontiki

import Control.Concurrent (ThreadId, forkIO, killThread, threadDelay)
import Control.Concurrent.STM
import Control.Monad.IO.Class (MonadIO (..))
import HaskKV.Types (Timeout (Timeout))
import System.Random (Random (randomR), getStdRandom)

timeoutRange :: Timeout -> (Int, Int)
timeoutRange (Timeout timeout) = (timeout, 2 * timeout - 1)

randTimeout :: Timeout -> IO Timeout
randTimeout = fmap Timeout . getStdRandom . randomR . timeoutRange

data Timer = Timer
  { _thread :: TMVar ThreadId
  , _var    :: TMVar ()
  }

new :: STM Timer
new = Timer <$> newEmptyTMVar <*> newEmptyTMVar

newIO :: IO Timer
newIO = Timer <$> newEmptyTMVarIO <*> newEmptyTMVarIO

reset :: (MonadIO m) => Timer -> Timeout -> m ()
reset t (Timeout timeout) = liftIO $ do
  cancel t

  n <- forkIO $ do
    threadDelay timeout
    atomically $ tryPutTMVar (_var t) ()
    return ()

  atomically $ putTMVar (_thread t) n

cancel :: (MonadIO m) => Timer -> m ()
cancel t = liftIO $ do
  tid <- atomically $ tryTakeTMVar (_thread t)
  maybe (pure ()) killThread tid

await :: Timer -> STM ()
await = takeTMVar . _var
