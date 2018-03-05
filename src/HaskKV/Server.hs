module HaskKV.Server where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Function (fix)
import HaskKV.Store (MemStore, Storable, execMemStoreTVar)
import Network.Socket
import System.IO

type Message = String

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
    hdl <- liftIO $ socketToHandle sock ReadWriteMode
    liftIO $ hSetBuffering hdl NoBuffering
    commLine <- liftIO $ dupChan chan

    -- Reads from broadcast channel and
    -- sends broadcast messages back to client.
    liftIO . forkIO . fix $ \loop -> do
        msg <- readChan commLine
        hPutStrLn hdl msg
        loop

    fix $ \loop -> do
        line <- liftIO . fmap init $ hGetLine hdl

        -- TODO(DarinM223): handle message from client

        liftIO $ hPutStrLn hdl line
        loop
