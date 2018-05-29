module ServerTest (tests) where

import Conduit
import Control.Concurrent (threadDelay)
import Control.Monad
import Data.ByteString
import Test.Tasty
import Test.Tasty.HUnit
import HaskKV.Utils

import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ
import qualified HaskKV.Timer as T
import qualified HaskKV.Server as S

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

backpressure :: S.Capacity
backpressure = S.Capacity 100

timeout :: T.Timeout
timeout = T.Timeout 100

longer :: T.Timeout
longer = T.Timeout 1000

unitTests :: TestTree
unitTests = testGroup "Unit tests"
    [ testCase "Receives message" testReceiveMessage
    , testCase "Sends message" testSendMessage
    , testCase "Broadcasts message" testBroadcastMessage
    , testCase "Resets election timer" testResetElection
    , testCase "Resets heartbeat timer" testResetHearbeat
    , testCase "Inject takes priority" testInject
    ]
  where
    testReceiveMessage :: IO ()
    testReceiveMessage = do
        state <- S.createServerState backpressure timeout longer
        sourceMessages (["a", "b"] :: [ByteString]) state
        msg1 <- S.recvImpl state
        liftIO $ msg1 @?= Right "a"
        msg2 <- S.recvImpl state
        liftIO $ msg2 @?= Right "b"
        msg3 <- S.recvImpl state
        liftIO $ msg3 @?= Left S.ElectionTimeout
        return ()

    testSendMessage :: IO ()
    testSendMessage = do
        state <- S.createServerState backpressure timeout timeout
        state <- buildClientMap state [1..2]
        S.sendImpl 1 "a" state
        S.sendImpl 2 "b" state
        results <- sinkClients state
        results @?= [["a"], ["b"]]

    testBroadcastMessage :: IO ()
    testBroadcastMessage = do
        state <- S.createServerState backpressure timeout timeout
        state <- buildClientMap state [1..3]
        S.broadcastImpl "a" state
        results <- sinkClients state
        results @?= [["a"], ["a"], ["a"]]

    testResetElection :: IO ()
    testResetElection = do
        state <- S.createServerState
            backpressure
            (T.Timeout 4000)
            (T.Timeout 5000) :: IO (S.ServerState ByteString)
        liftIO $ threadDelay 3000
        S.resetImpl S.ElectionTimeout state
        msg1 <- S.recvImpl state
        liftIO $ msg1 @?= Left S.HeartbeatTimeout
        msg2 <- S.recvImpl state
        liftIO $ msg2 @?= Left S.ElectionTimeout
        return ()

    testResetHearbeat :: IO ()
    testResetHearbeat = do
        state <- S.createServerState
            backpressure
            (T.Timeout 5000)
            (T.Timeout 4000) :: IO (S.ServerState ByteString)
        liftIO $ threadDelay 3000
        S.resetImpl S.HeartbeatTimeout state
        msg1 <- S.recvImpl state
        liftIO $ msg1 @?= Left S.ElectionTimeout
        msg2 <- S.recvImpl state
        liftIO $ msg2 @?= Left S.HeartbeatTimeout
        return ()

    testInject :: IO ()
    testInject = do
        state <- S.createServerState
            backpressure
            (T.Timeout 5000)
            (T.Timeout 10000) :: IO (S.ServerState ByteString)
        S.injectImpl S.HeartbeatTimeout state
        msg1 <- S.recvImpl state
        liftIO $ msg1 @?= Left S.HeartbeatTimeout
        msg2 <- S.recvImpl state
        liftIO $ msg2 @?= Left S.ElectionTimeout
        return ()

sourceMessages l s
    = runConduit
    $ CL.sourceList l
   .| sinkRollingQueue (S._messages s)

buildClientMap = foldM addToMap
  where
    addToMap s i = do
        rq <- RQ.newIO 100
        return s { S._outgoing = IM.insert i rq . S._outgoing $ s }

sinkClients s =
    forM (IM.assocs . S._outgoing $ s) $ \(_, rq) ->
          runConduit
        $ sourceRQOne rq
       .| sinkList
