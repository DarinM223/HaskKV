module HaskKV.Server where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Binary
import Data.Function (fix)
import Data.Int
import HaskKV.Log (LogEntry)
import HaskKV.State (RaftMessage)
import HaskKV.Store (MemStore, Storable, execMemStoreTVar)
import Network.Socket

import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

type MsgLen = Word16
type Message = RaftMessage (LogEntry Int Int)

msgLenLen :: Int64
msgLenLen = 2

isEOT :: MsgLen -> Bool
isEOT = (== 0)

sendMessage :: Socket -> Message -> IO ()
sendMessage sock msg = do
    NBS.send sock encodedLen
    NBS.send sock encodedMsg
    return ()
  where
    encodedMsg = encode msg
    msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
    encodedLen = encode msgLen

recvMessage :: Socket -> IO (Maybe Message)
recvMessage sock = do
    msgLenS <- liftIO $ NBS.recv sock msgLenLen
    let msgLen = decode msgLenS :: MsgLen
    if (isEOT msgLen)
        then do
            msg <- liftIO $ NBS.recv sock $ fromIntegral msgLen
            let message = decode msg :: Message
            return $ Just message
        else return Nothing

runServer :: (Ord k, Storable v) => TVar (MemStore k v) -> IO ()
runServer var = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 4242 iNADDR_ANY)
    listen sock 2

    chan <- newChan

    -- TODO(DarinM223): read config and fork threads to handle every worker
    -- in the config.

    mainLoop sock chan var

mainLoop :: (Ord k, Storable v)
         => Socket
         -> Chan Message
         -> TVar (MemStore k v)
         -> IO ()
mainLoop sock chan var = do
    conn <- accept sock
    forkIO $ execMemStoreTVar (runConn chan conn) var
    mainLoop sock chan var

runConn :: (MonadIO m)
        => Chan Message
        -> (Socket, SockAddr)
        -> m ()
runConn chan (sock, _) = do
    commLine <- liftIO $ dupChan chan

    -- Reads from broadcast channel and
    -- sends broadcast messages back to client.
    liftIO . forkIO . fix $ \loop -> do
        msg <- readChan commLine
        liftIO $ sendMessage sock msg
        loop

    fix $ \loop -> do
        msgMaybe <- liftIO $ recvMessage sock
        mapM_ (liftIO . sendMessage sock) msgMaybe
        loop
