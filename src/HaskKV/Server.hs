module HaskKV.Server where

import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad.Reader
import HaskKV.Log (LogM, LogME)
import HaskKV.Store (StorageM, StorageMK, StorageMKV)

import qualified Network.Socket as S

class (Monad m) => ServerM msg m where
    send      :: msg -> m ()
    broadcast :: msg -> m ()
    recv      :: m msg

    default send :: (MonadTrans t, ServerM msg m', m ~ t m') => msg -> m ()
    send = lift . send

    default broadcast :: (MonadTrans t, ServerM msg m', m ~ t m') => msg -> m ()
    broadcast = lift . send

    default recv :: (MonadTrans t, ServerM msg m', m ~ t m') => m msg
    recv = lift recv

data ServerState msg = ServerState
    { _socket    :: S.Socket
    , _broadcast :: Chan msg
    , _sendLock  :: MVar ()
    }

newtype ServerT msg m a = ServerT { unServerT :: ReaderT (ServerState msg) m a }
    deriving ( Functor, Applicative, Monad, MonadIO, MonadTrans
             , MonadReader (ServerState msg)
             )

instance (MonadIO m) => ServerM msg (ServerT msg m) where
    send = undefined
    broadcast msg = do
        chan <- _broadcast <$> ask
        liftIO $ writeChan chan msg
    recv = undefined

instance (StorageM m) => StorageM (ServerT msg m)
instance (StorageMK k m) => StorageMK k (ServerT msg m)
instance (StorageMKV k v m) => StorageMKV k v (ServerT msg m)
instance (LogM m) => LogM (ServerT msg m)
instance (LogME e m) => LogME e (ServerT msg m)
instance (ServerM msg m) => ServerM msg (ReaderT r m)
