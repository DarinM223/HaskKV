module ServerTest (tests) where

import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Data.Binary
import Network.Socket hiding (send, recv)
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Server

import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ withResource (openSocket 4242) close $ \getRecvSock ->
      withResource (openSocket 4343) close $ \getSendSock -> testCase "Receives message" $ do
        sendSock <- getSendSock
        recvSock <- getRecvSock
        lock <- newMVar ()
        chan <- newChan
        let state = ServerState { _socket = sock, _broadcast = chan, _sendLock = lock }
        flip execServerT state $ do
            let msg = 2 :: Int
                encodedMsg = encode msg
                msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
                encodedLen = encode msgLen
            liftIO $ NBS.send sock encodedLen
            liftIO $ NBS.send sock encodedMsg
            maybeMsg <- recv
            liftIO $ maybeMsg @?= Just (2 :: Int)
        return ()
    ]

openSocket :: PortNumber -> IO Socket
openSocket addr = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet addr iNADDR_ANY)
    listen sock 1
    return sock
