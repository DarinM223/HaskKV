{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad.Reader
import Data.Int
import Data.Word
import Data.Binary
import HaskKV.Log (LogM)
import HaskKV.Store (StorageM)

import qualified Network.Socket as S
import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

class (Monad m) => ServerM msg m | m -> msg where
    send      :: msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m (Maybe msg)

    default send :: (MonadTrans t, ServerM msg m', m ~ t m') => msg -> m ()
    send = lift . send

    default broadcast :: (MonadTrans t, ServerM msg m', m ~ t m') => msg -> m ()
    broadcast = lift . send

    default recv :: (MonadTrans t, ServerM msg m', m ~ t m') => m (Maybe msg)
    recv = lift recv

data ServerState msg = ServerState
    { _socket    :: S.Socket
    , _broadcast :: Chan msg
    , _sendLock  :: MVar ()
    }

type MsgLen = Word16

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
    liftIO $ forkIO $ listenToBroadcast s
    runReaderT (unServerT m) s

instance (MonadIO m, Binary msg) => ServerM msg (ServerT msg m) where
    send msg = liftIO . sendMessage msg =<< ask

    broadcast msg = do
        chan <- _broadcast <$> ask
        liftIO $ writeChan chan msg

    recv = do
        sock <- _socket <$> ask
        msgLenS <- liftIO $ NBS.recv sock msgLenLen
        let msgLen = decode msgLenS :: MsgLen
        if not $ isEOT msgLen
            then do
                msg <- liftIO $ NBS.recv sock $ fromIntegral msgLen
                let message = decode msg :: msg
                return $ Just message
            else return Nothing

instance (StorageM k v m) => StorageM k v (ServerT msg m)
instance (LogM e m) => LogM e (ServerT msg m)
instance (ServerM msg m) => ServerM msg (ReaderT r m)

listenToBroadcast :: (Binary msg) => ServerState msg -> IO ()
listenToBroadcast s = do
    let chan = _broadcast s
    msg <- readChan chan
    sendMessage msg s
    listenToBroadcast s

sendMessage :: (Binary msg) => msg -> ServerState msg -> IO ()
sendMessage msg s =
    withMVar (_sendLock s) $ \_ -> do
        NBS.send (_socket s) encodedLen
        NBS.send (_socket s) encodedMsg
        return ()
  where
    encodedMsg = encode msg
    msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
    encodedLen = encode msgLen

msgLenLen :: Int64
msgLenLen = 2

isEOT :: MsgLen -> Bool
isEOT = (== 0)
