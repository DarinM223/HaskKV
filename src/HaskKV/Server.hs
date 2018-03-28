{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server where

import Control.Concurrent (forkIO)
import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary
import Data.Conduit
import Data.Conduit.Network
import HaskKV.Log (LogM)
import HaskKV.Store (StorageM)
import HaskKV.Utils

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ
import qualified HaskKV.Timer as Timer

class (Monad m) => ServerM msg e m | m -> msg e where
    send      :: Int -> msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m (Either e msg)
    reset     :: e -> m ()

    default send :: (MonadTrans t, ServerM msg e m', m ~ t m') => Int -> msg -> m ()
    default broadcast :: (MonadTrans t, ServerM msg e m', m ~ t m') => msg -> m ()
    default recv :: (MonadTrans t, ServerM msg e m', m ~ t m') => m (Either e msg)
    default reset :: (MonadTrans t, ServerM msg e m', m ~ t m') => e -> m ()

    send i m = lift $ send i m
    broadcast = lift . broadcast
    recv = lift recv
    reset = lift . reset

newtype Capacity = Capacity { unCapacity :: Int }

data ServerState msg = ServerState
    { _messages         :: RQ.RollingQueue msg
    , _outgoing         :: IM.IntMap (RQ.RollingQueue msg)
    , _electionTimer    :: Timer.Timer
    , _heartbeatTimer   :: Timer.Timer
    , _electionTimeout  :: Timer.Timeout
    , _heartbeatTimeout :: Timer.Timeout
    }

createServerState :: Capacity
                  -> Timer.Timeout
                  -> Timer.Timeout
                  -> IO (ServerState msg)
createServerState backpressure electionTimeout heartbeatTimeout = do
    messages <- RQ.newIO (unCapacity backpressure)
    electionTimer <- Timer.newIO
    heartbeatTimer <- Timer.newIO
    return ServerState
        { _messages         = messages
        , _outgoing         = IM.empty
        , _electionTimer    = electionTimer
        , _heartbeatTimer   = heartbeatTimer
        , _electionTimeout  = electionTimeout
        , _heartbeatTimeout = heartbeatTimeout
        }

runServer :: (Binary msg)
          => Int
          -> HostPreference
          -> IM.IntMap ClientSettings
          -> ServerState msg
          -> IO ()
runServer port host clients s = do
    liftIO $ forkIO $ runTCPServer (serverSettings port host) $ \appData ->
        runConduit
            $ appSource appData
           .| CL.mapMaybe (decode . BL.fromStrict)
           .| sinkRollingQueue (_messages s)

    forM_ (IM.assocs clients) $ \(i, settings) -> do
        forM_ (IM.lookup i . _outgoing $ s) $ \rq -> do
            liftIO $ forkIO $ runTCPClient settings $ \appData ->
                runConduit
                    $ sourceRollingQueue rq
                   .| CL.map (B.concat . BL.toChunks . encode)
                   .| appSink appData

data ServerEvent = ElectionTimeout
                 | HeartbeatTimeout
                 deriving (Show, Eq)

newtype ServerT msg m a = ServerT { unServerT :: ReaderT (ServerState msg) m a }
    deriving
        ( Functor, Applicative, Monad, MonadIO, MonadTrans
        , MonadReader (ServerState msg)
        )

runServerT :: (MonadIO m)
           => ServerT msg m a
           -> ServerState msg
           -> m a
runServerT m s = do
    liftIO $ Timer.reset (_electionTimer s) (_electionTimeout s)
    liftIO $ Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
    runReaderT (unServerT m) s

instance (MonadIO m) => ServerM msg ServerEvent (ServerT msg m) where
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
            awaitElectionTimeout s
            `orElse` awaitHeartbeatTimeout s
            `orElse` (Right . fst <$> RQ.read (_messages s))
        return msg
      where
        awaitElectionTimeout
            = fmap (const (Left ElectionTimeout))
            . Timer.await
            . _electionTimer
        awaitHeartbeatTimeout
            = fmap (const (Left HeartbeatTimeout))
            . Timer.await
            . _heartbeatTimer

    reset HeartbeatTimeout = ask >>= \s ->
        liftIO $ Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
    reset ElectionTimeout = ask >>= \s ->
        liftIO $ Timer.reset (_electionTimer s) (_electionTimeout s)

instance (StorageM k v m) => StorageM k v (ServerT msg m)
instance (LogM e m) => LogM e (ServerT msg m)
instance (ServerM msg e m) => ServerM msg e (ReaderT r m)
