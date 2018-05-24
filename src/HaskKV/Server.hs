{-# LANGUAGE UndecidableInstances #-}

module HaskKV.Server where

import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.Binary
import Data.Conduit
import Data.Conduit.Network
import Data.Streaming.Network.Internal
import HaskKV.Log (LogM, TempLogM)
import HaskKV.Store (ApplyEntryM, StorageM)
import HaskKV.Utils
import System.Log.Logger

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
    inject    :: e -> m ()
    serverIds :: m [Int]

    default send :: (MonadTrans t, ServerM msg e m', m ~ t m') => Int -> msg -> m ()
    default broadcast :: (MonadTrans t, ServerM msg e m', m ~ t m') => msg -> m ()
    default recv :: (MonadTrans t, ServerM msg e m', m ~ t m') => m (Either e msg)
    default reset :: (MonadTrans t, ServerM msg e m', m ~ t m') => e -> m ()
    default inject :: (MonadTrans t, ServerM msg e m', m ~ t m') => e -> m ()
    default serverIds :: (MonadTrans t, ServerM msg e m', m ~ t m') => m [Int]

    send i m = lift $ send i m
    broadcast = lift . broadcast
    recv = lift recv
    reset = lift . reset
    inject = lift . inject
    serverIds = lift serverIds

newtype Capacity = Capacity { unCapacity :: Int } deriving (Show, Eq)

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

    Timer.reset electionTimer electionTimeout
    Timer.reset heartbeatTimer heartbeatTimeout

    return ServerState
        { _messages         = messages
        , _outgoing         = IM.empty
        , _electionTimer    = electionTimer
        , _heartbeatTimer   = heartbeatTimer
        , _electionTimeout  = electionTimeout
        , _heartbeatTimeout = heartbeatTimeout
        }

retryTimeout :: Int
retryTimeout = 1000000

runServer :: (Binary msg, Show msg)
          => Int
          -> HostPreference
          -> IM.IntMap ClientSettings
          -> ServerState msg
          -> IO ()
runServer port host clients s = do
    forkIO $ runTCPServer (serverSettings port host) $ \appData ->
        runConduitRes
            $ appSource appData
           .| CL.mapFoldable (fmap thrd . decodeOrFail . BL.fromStrict)
           .| CL.iterM (liftIO . debugM "conduit" . ("Receiving: " ++) . show)
           .| sinkRollingQueue (_messages s)

    forM_ (IM.assocs clients) $ \(i, settings) -> do
        forM_ (IM.lookup i . _outgoing $ s) $ \rq -> do
            forkIO $ connectClient settings rq
  where
    thrd t = let (_, _, a) = t in a

    connectClient settings rq = do
        let connect appData = putStrLn "Connected to server"
                           >> connectStream rq appData

        debugM "conduit" $ "Connecting to host "
                        ++ show (clientHost settings)
                        ++ " and port "
                        ++ show (clientPort settings)

        catch (runTCPClient settings connect) $ \(_ :: SomeException) -> do
            putStrLn "Error connecting to server, retrying"
            threadDelay retryTimeout
            connectClient settings rq

    connectStream rq appData
        = runConduit
        $ sourceRollingQueue rq
       .| CL.iterM (liftIO . debugM "conduit" . ("Sending: " ++) . show)
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

runServerT :: ServerT msg m a -> ServerState msg -> m a
runServerT m s = runReaderT (unServerT m) s

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

    inject HeartbeatTimeout = ask >>= \s ->
        liftIO $ Timer.reset (_heartbeatTimer s) (Timer.Timeout 0)
    inject ElectionTimeout = ask >>= \s ->
        liftIO $ Timer.reset (_electionTimer s) (Timer.Timeout 0)

    reset HeartbeatTimeout = ask >>= \s ->
        liftIO $ Timer.reset (_heartbeatTimer s) (_heartbeatTimeout s)
    reset ElectionTimeout = do
        s <- ask
        timeout <- liftIO $ Timer.randTimeout $ _electionTimeout s
        liftIO $ Timer.reset (_electionTimer s) timeout

    serverIds = IM.keys . _outgoing <$> ask

instance (StorageM k v m) => StorageM k v (ServerT msg m)
instance (ApplyEntryM k v e m) => ApplyEntryM k v e (ServerT msg m)
instance (LogM e m) => LogM e (ServerT msg m)
instance (TempLogM e m) => TempLogM e (ServerT msg m)
instance (ServerM msg e m) => ServerM msg e (ReaderT r m)
instance (ServerM msg e m) => ServerM msg e (StateT s m)
