module HaskKV.TCPConn where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Monad.IO.Class
import Network.Socket

runServer :: PortNumber
          -> (Socket -> p)
          -> (p -> IO ())
          -> (Chan msg -> p -> IO ())
          -> IO ()
runServer port makeParams initServer handler = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet port iNADDR_ANY)
    listen sock 2

    chan <- newChan
    let params = makeParams sock

    -- TODO(DarinM223): read config and fork threads to handle every worker
    -- in the config.
    initServer params

    mainLoop sock chan params handler

mainLoop :: Socket -> Chan msg -> p -> (Chan msg -> p -> IO ()) -> IO ()
mainLoop sock chan params handler = do
    conn <- accept sock
    commLine <- liftIO $ dupChan chan
    -- TODO(DarinM223): use copy of params with channel as commLine
    forkIO $ handler commLine params
    mainLoop sock chan params handler
