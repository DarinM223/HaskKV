module HaskKV.Timer where

-- Implementation taken from: https://github.com/NicolasT/kontiki

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad.IO.Class

newtype Timeout = Timeout { unTimeout :: Int }

data Timer = Timer
    { _thread :: TMVar ThreadId
    , _var    :: TMVar ()
    }

new :: STM Timer
new = do
    thread <- newEmptyTMVar
    var <- newEmptyTMVar
    return $ Timer thread var

newIO :: IO Timer
newIO = do
    thread <- newEmptyTMVarIO
    var <- newEmptyTMVarIO
    return $ Timer thread var

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
    mapM_ killThread tid

await :: Timer -> STM ()
await = takeTMVar . _var
