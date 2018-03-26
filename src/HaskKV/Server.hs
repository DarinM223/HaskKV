{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import Data.ByteString.Lazy
import Data.Conduit
import Data.Conduit.Network
import HaskKV.Log (LogM)
import HaskKV.Store (StorageM)
import HaskKV.Utils

import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ
import qualified HaskKV.Timer as Timer

class (Monad m) => ServerM msg e m | m -> msg e where
    send      :: Int -> msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m (Either e msg)

    default send :: (MonadTrans t, ServerM msg e m', m ~ t m') => Int -> msg -> m ()
    default broadcast :: (MonadTrans t, ServerM msg e m', m ~ t m') => msg -> m ()
    default recv :: (MonadTrans t, ServerM msg e m', m ~ t m') => m (Either e msg)

    send i m = lift $ send i m
    broadcast = lift . broadcast
    recv = lift recv

data ServerState msg = ServerState
    { _messages :: RQ.RollingQueue msg
    , _timer    :: Timer.Timer
    , _outgoing :: IM.IntMap (RQ.RollingQueue msg)
    , _timeout  :: Int
    }

createServerState :: Int -> Int -> IO (ServerState msg)
createServerState backpressure timeout = do
    messages <- RQ.newIO backpressure
    timer <- Timer.newIO
    return ServerState
        { _messages = messages
        , _timer    = timer
        , _outgoing = IM.empty
        , _timeout  = timeout
        }

data ServerError = Timeout
                 | EOF
                 deriving (Show, Eq)

newtype ServerT msg m a = ServerT { unServerT :: ReaderT (ServerState msg) m a }
    deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (ServerState msg)
        )

execServerT :: (MonadIO m, Binary msg)
            => ServerT msg m a
            -> ServerState msg
            -> m a
execServerT m s = do
    liftIO $ forkIO $ runTCPServer (serverSettings 4000 "*") $ \appData ->
        runConduit
            $ appSource appData
           .| CL.mapMaybe (decode . fromStrict)
           .| sinkRollingQueue (_messages s)

    liftIO $ Timer.reset (_timer s) (_timeout s)
    runReaderT (unServerT m) s

instance
    ( MonadIO m
    , Binary msg
    ) => ServerM msg ServerError (ServerT msg m) where

    send = undefined
    broadcast = undefined
    recv = do
        s <- ask
        msg <- liftIO . atomically $
            (const (Left Timeout) <$> Timer.await (_timer s))
            `orElse` (Right . fst <$> RQ.read (_messages s))
        liftIO $ Timer.reset (_timer s) (_timeout s)
        return msg

{-instance (MonadIO m, Binary msg) => ServerM msg (ServerT msg m) where-}
{-    -- TODO(DarinM223): write to specific socket (refactor to take in-}
{-    -- server id)-}
{-    send msg = liftIO . sendMessage msg =<< ask-}

{-    -- TODO(DarinM223): write to all sockets-}
{-    broadcast msg = do-}
{-        chan <- _broadcast <$> ask-}
{-        liftIO $ writeChan chan msg-}

{-    -- TODO(DarinM223): read from -}
{-    recv = do-}
{-        sock <- _socket <$> ask-}
{-        msgLenS <- liftIO $ NBS.recv sock msgLenLen-}
{-        let msgLen = decode msgLenS :: MsgLen-}
{-        if not $ isEOT msgLen-}
{-            then do-}
{-                msg <- liftIO $ NBS.recv sock $ fromIntegral msgLen-}
{-                let message = decode msg :: msg-}
{-                return $ Just message-}
{-            else return Nothing-}

{-    serverID = _serverIdx <$> ask-}

instance (StorageM k v m) => StorageM k v (ServerT msg m)
instance (LogM e m) => LogM e (ServerT msg m)
instance (ServerM msg e m) => ServerM msg e (ReaderT r m)

{-listenToBroadcast :: (Binary msg) => ServerState msg -> IO ()-}
{-listenToBroadcast s = do-}
{-    let chan = _broadcast s-}
{-    msg <- readChan chan-}
{-    sendMessage msg s-}
{-    listenToBroadcast s-}

{-sendMessage :: (Binary msg) => msg -> ServerState msg -> IO ()-}
{-sendMessage msg s =-}
{-    withMVar (_sendLock s) $ \_ -> do-}
{-        NBS.send (_socket s) encodedLen-}
{-        NBS.send (_socket s) encodedMsg-}
{-        return ()-}
{-  where-}
{-    encodedMsg = encode msg-}
{-    msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen-}
{-    encodedLen = encode msgLen-}

{-msgLenLen :: Int64-}
{-msgLenLen = 2-}

{-isEOT :: MsgLen -> Bool-}
{-isEOT = (== 0)-}
