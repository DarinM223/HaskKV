module ServerTest (tests) where

import Control.Monad.IO.Class
import Control.Monad
import Data.ByteString
import Test.Tasty
import Test.Tasty.HUnit
import qualified HaskKV.Server as S
import HaskKV.Utils

import Conduit
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM
import qualified Data.STM.RollingQueue as RQ

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
    testReceiveMessage = do
        state <- S.createServerState 100 100
        sourceMessages (["a", "b"] :: [ByteString]) state
        flip S.runServerT state $ do
            msg1 <- S.recv
            liftIO $ msg1 @?= Right "a"
            msg2 <- S.recv
            liftIO $ msg2 @?= Right "b"
            msg3 <- S.recv
            liftIO $ msg3 @?= Left S.Timeout
        return ()

    testSendMessage :: IO ()
    testSendMessage = do
        state <- S.createServerState 100 100
        state' <- buildClientMap state [1..2]
        flip S.runServerT state' $ do
            S.send 1 "a"
            S.send 2 "b"
        results <- sinkClients state'
        results @?= [["a"], ["b"]]

    testBroadcastMessage :: IO ()
    testBroadcastMessage = do
        state <- S.createServerState 100 100
        state' <- buildClientMap state [1..3]
        S.runServerT (S.broadcast "a") state'
        results <- sinkClients state'
        results @?= [["a"], ["a"], ["a"]]

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
