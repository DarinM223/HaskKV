module ServerTest (tests) where

import Control.Concurrent (threadDelay)
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
import qualified Network.Socket as S

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "Receives message" testReceiveMessage
    , testCase "Sends message" testSendMessage
    , testCase "Broadcasts message" testBroadcastMessage
    ]
  where
    testReceiveMessage :: IO ()
    testReceiveMessage = runTest (server "4242") (client "4242" sendBasic)

    testSendMessage :: IO ()
    testSendMessage = runTest (server "4243") (client "4243" sendServerM)

    testBroadcastMessage :: IO ()
    testBroadcastMessage = do
        lock1 <- newEmptyMVar
        lock2 <- newEmptyMVar
        lock3 <- newEmptyMVar
        broadcastChan <- newChan
        chan1 <- dupChan broadcastChan
        chan2 <- dupChan broadcastChan
        chan3 <- dupChan broadcastChan
        withAsync (broadcastServer "4244" chan1 lock1) $ \asyncServer1 ->
          withAsync (broadcastServer "4245" chan2 lock2) $ \asyncServer2 ->
            withAsync (broadcastServer "4246" chan3 lock3) $ \asyncServer3 ->
              withAsync (client "4244" readServerM lock1) $ \asyncClient1 ->
                withAsync (client "4245" readServerM lock2) $ \asyncClient2 ->
                  withAsync (client "4246" readServerM lock3) $ \asyncClient3 -> do
                    writeChan broadcastChan (2 :: Int)
                    wait asyncServer1
                    wait asyncServer2
                    wait asyncServer3
                    wait asyncClient1
                    wait asyncClient2
                    wait asyncClient3
        return ()

    runTest :: (MVar () -> IO a) -> (MVar () -> IO b) -> IO ()
    runTest serverFn clientFn = do
        lock <- newEmptyMVar
        withAsync (serverFn lock) $ \asyncServer ->
            withAsync (clientFn lock) $ \asyncClient -> do
                wait asyncServer
                wait asyncClient
        return ()

    client :: String -> (MVar () -> S.Socket -> IO ()) -> MVar () -> IO ()
    client port fn lock = do
        readMVar lock
        connect "127.0.0.1" port $ \(sock, _) -> fn lock sock

    sendBasic :: MVar () -> S.Socket -> IO ()
    sendBasic lock sock =
        withMVar lock $ \_ -> do
            NBS.send sock encodedLen
            NBS.send sock encodedMsg
            return ()
      where
        msg = 2 :: Int
        encodedMsg = encode msg
        msgLen = fromIntegral $ BS.length encodedMsg :: MsgLen
        encodedLen = encode msgLen

    sendServerM :: MVar () -> S.Socket -> IO ()
    sendServerM lock sock = do
        chan <- newChan
        let state = ServerState { _socket = sock, _broadcast = chan, _sendLock = lock }
        flip execServerT state $ send (2 :: Int)

    readServerM :: MVar () -> S.Socket -> IO ()
    readServerM lock sock = do
        chan <- newChan
        let state = ServerState { _socket = sock, _broadcast = chan, _sendLock = lock }
        flip execServerT state $ do
            msg <- recv
            liftIO $ msg @?= Just (2 :: Int)
            return ()

    server :: String -> MVar () -> IO ()
    server port lock = listen "127.0.0.1" port $ \(lsock, _) -> do
        putMVar lock ()
        accept lsock $ \(csock, _) -> do
            chan <- newChan
            let state = ServerState { _socket = csock, _broadcast = chan, _sendLock = lock }
            flip execServerT state $ do
                msg <- recv
                liftIO $ msg @?= Just (2 :: Int)
        return ()

    broadcastServer :: String -> Chan Int -> MVar () -> IO ()
    broadcastServer port chan lock = listen "127.0.0.1" port $ \(lsock, _) -> do
        putMVar lock ()
        accept lsock $ \(csock, _) -> do
            let state = ServerState { _socket = csock, _broadcast = chan, _sendLock = lock }
            flip execServerT state $ liftIO $ threadDelay 500000
