{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
module ServerTest (tests) where

import Conduit
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM
import Control.Monad (foldM)
import Data.ByteString
import Data.Traversable (for)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit
import HaskKV.Utils
import HaskKV.Types
import Optics

import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified HaskKV.Server.All as S
import qualified HaskKV.Server.Instances as SI

tests :: TestTree
tests = testGroup "Server tests" [unitTests]

backpressure :: Capacity
backpressure = Capacity 100

timeout :: Timeout
timeout = 100

longer :: Timeout
longer = 1000

unitTests :: TestTree
unitTests = testGroup
  "Unit tests"
  [ testReceiveMessage
  , testSendMessage
  , testBroadcastMessage
  , testResetElection
  , testResetHearbeat
  , testInject
  ]

testReceiveMessage :: TestTree
testReceiveMessage = testCase "Receives message" $ do
  state <- S.newServerState backpressure timeout longer 1
  sourceMessages (["a", "b"] :: [ByteString]) state
  msg1 <- SI.recv state
  msg1 @?= Right "a"
  msg2 <- SI.recv state
  msg2 @?= Right "b"
  msg3 <- SI.recv state
  msg3 @?= Left S.ElectionTimeout

testSendMessage :: TestTree
testSendMessage = testCase "Sends message" $ do
  state <- S.newServerState backpressure timeout timeout 1
  state <- buildClientMap state [1 .. 2]
  SI.send 1 "a" state
  SI.send 2 "b" state
  results <- sinkClients state
  results @?= [["a"], ["b"]]

testBroadcastMessage :: TestTree
testBroadcastMessage = testCase "Broadcasts message" $ do
  state <- S.newServerState backpressure timeout timeout 1
  state <- buildClientMap state [1 .. 3]
  SI.broadcast "a" state
  results <- sinkClients state
  results @?= [[], ["a"], ["a"]]

testResetElection :: TestTree
testResetElection = testCase "Resets election timer" $ do
  state <-
    S.newServerState backpressure 500000 600000 1 :: IO
      (S.ServerState ByteString)
  forkIO $ do
    threadDelay 400000
    SI.reset S.ElectionTimeout state
  msg1 <- SI.recv state
  msg1 @?= Left S.HeartbeatTimeout
  msg2 <- SI.recv state
  msg2 @?= Left S.ElectionTimeout

testResetHearbeat :: TestTree
testResetHearbeat = testCase "Resets heartbeat timer" $ do
  state <-
    S.newServerState backpressure 600000 500000 1 :: IO
      (S.ServerState ByteString)
  forkIO $ do
    threadDelay 400000
    SI.reset S.HeartbeatTimeout state
  msg1 <- SI.recv state
  msg1 @?= Left S.ElectionTimeout
  msg2 <- SI.recv state
  msg2 @?= Left S.HeartbeatTimeout

testInject :: TestTree
testInject = testCase "Inject takes priority" $ do
  state <-
    S.newServerState backpressure 5000 10000 1 :: IO (S.ServerState ByteString)
  SI.inject S.HeartbeatTimeout state
  msg1 <- SI.recv state
  msg1 @?= Left S.HeartbeatTimeout
  msg2 <- SI.recv state
  msg2 @?= Left S.ElectionTimeout

sourceMessages l s =
  runConduit $ CL.sourceList l .| sinkTBQueue (s ^. #messages)

buildClientMap = foldM addToMap
 where
  addToMap s i = do
    bq <- newTBQueueIO 100
    return $ s & #outgoing %~ IM.insert i bq

sinkClients s = for (IM.assocs $ s ^. #outgoing) $ \(_, bq) ->
  atomically (isEmptyTBQueue bq) >>= \case
    True  -> return []
    False -> runConduit $ sourceTBQueueOne bq .| sinkList
