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
    -- TODO(DarinM223): separate this out so that tests can use a different
    -- conduit sink/source (lists, etc)
    liftIO $ forkIO $ runTCPServer (serverSettings 4000 "*") $ \appData ->
        runConduit
            $ appSource appData
           .| CL.mapMaybe (decode . fromStrict)
           .| sinkRollingQueue (_messages s)

    -- TODO(DarinM223): for every RollingQueue in outgoing, sink it to the
    -- appropriate socket.

    liftIO $ Timer.reset (_timer s) (_timeout s)
    runReaderT (unServerT m) s

instance (MonadIO m) => ServerM msg ServerError (ServerT msg m) where
    send i msg = do
        s <- ask
        let rq = IM.lookup i . _outgoing $ s
        mapM_ (liftIO . atomically . flip RQ.write msg) rq

    broadcast msg
        = liftIO
        . atomically
        . mapM_ (flip RQ.write msg)
        . IM.elems
        . _outgoing
      =<< ask

    recv = do
        s <- ask
        msg <- liftIO . atomically $
            (const (Left Timeout) <$> Timer.await (_timer s))
            `orElse` (Right . fst <$> RQ.read (_messages s))
        liftIO $ Timer.reset (_timer s) (_timeout s)
        return msg

instance (StorageM k v m) => StorageM k v (ServerT msg m)
instance (LogM e m) => LogM e (ServerT msg m)
instance (ServerM msg e m) => ServerM msg e (ReaderT r m)
