module ServerTest (tests) where

import Control.Concurrent.Async (wait, withAsync)
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Monad.IO.Class
import Data.Binary
import Network.Simple.TCP hiding (send, recv)
import Test.Tasty
import Test.Tasty.HUnit

import HaskKV.Server

import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "Receives message" $ do
        lock <- newEmptyMVar
        withAsync (server lock) $ \asyncServer ->
            withAsync (client lock) $ \asyncClient -> do
                wait $ asyncServer
                wait $ asyncClient
    ]
  where
    client :: MVar () -> IO ()
    client lock = do
        readMVar lock
        connect "127.0.0.1" "4242" $ \(sock, _) -> do
            withMVar lock $ \_ -> do
                NBS.send sock encodedLen
                NBS.send sock encodedMsg
                return ()
      where
        msg = 2 :: Int
        encodedMsg = encode msg
        msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
        encodedLen = encode msgLen

    server :: MVar () -> IO ()
    server lock = listen "127.0.0.1" "4242" $ \(lsock, _) -> do
        putMVar lock ()
        accept lsock $ \(csock, _) -> do
            msgLenS <- NBS.recv csock 2
            let msgLen = decode msgLenS :: Word16
            msgS <- NBS.recv csock $ fromIntegral msgLen
            let msg = decode msgS :: Int
            msg @?= 2
            {-chan <- newChan-}
            {-let state = ServerState { _socket = csock, _broadcast = chan, _sendLock = lock }-}
            {-flip execServerT state $ do-}
            {-    msg <- recv-}
            {-    liftIO $ msg @?= Just (2 :: Int)-}
        return ()
