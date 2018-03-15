module ServerTest (tests) where

import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Data.Binary
import Network.Socket hiding (send, recv)
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Server
import HaskKV.TCPConn

import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

testPort :: PortNumber
testPort = 4242

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "Receives message" $ do
        lock <- newMVar ()
        chan <- newChan
        let makeParams = \sock -> ServerState { _socket = sock, _broadcast = chan, _sendLock = lock }
            handler = \_ state -> flip execServerT state $ do
                maybeMsg <- recv
                liftIO $ maybeMsg @?= Just (2 :: Int)
        runServer testPort makeParams (\_ -> return ()) handler
        sendSock <- connectSocket "localhost" testPort
        let msg = 2 :: Int
            encodedMsg = encode msg
            msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
            encodedLen = encode msgLen
        NBS.send sendSock encodedLen
        NBS.send sendSock encodedMsg
        return ()
    ]

connectSocket :: String -> PortNumber -> IO Socket
connectSocket host port = do
    sock <- socket AF_INET Stream 0
    connect sock (SockAddrInet port iNADDR_ANY)
    return sock
