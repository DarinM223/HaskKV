{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module Mock where

import Control.Monad.State
import Data.Foldable (foldl', for_)
import Data.Maybe
import HaskKV.Raft.Run
import HaskKV.Types
import Mock.Instances
import Optics
import Optics.State.Operators

import qualified Data.IntMap as IM

serverKeys :: MockT (IM.IntMap MockConfig) [Int]
serverKeys = MockT $ gets IM.keys

crashServer :: Int -> MockT (IM.IntMap MockConfig) ()
crashServer sid = MockT $ modify $ IM.delete sid

dropMessage :: Int -> MockT (IM.IntMap MockConfig) ()
dropMessage i = void $ runServer i $ MockT $ #receivingMsgs %= drop 1

hasEvent :: Int -> MockT (IM.IntMap MockConfig) Bool
hasEvent i = do
  has <- runServer i $ MockT $ do
    messages  <- use #receivingMsgs
    heartbeat <- use #heartbeatTimer
    election  <- use #electionTimer
    return $ (not $ null messages) || heartbeat || election
  return $ fromMaybe False has

runServer :: Int -> MockT MockConfig a -> MockT (IM.IntMap MockConfig) (Maybe a)
runServer i m = MockT (preuse (ix i)) >>= \case
  Just s -> do
    let (a, s') = runMockT m s
    MockT (ix i .= s')
    return $ Just a
  _ -> return Nothing

flushMessages :: Int -> MockT (IM.IntMap MockConfig) ()
flushMessages i = MockT (preuse (ix i)) >>= \case
  Just s -> MockT $ do
    let messages = sendingMsgs s
    ix i % #sendingMsgs .= []
    for_ messages $ \((SID sid), msg) -> ix sid % #receivingMsgs %= (++ [msg])
  _ -> return ()

runServers :: MockT (IM.IntMap MockConfig) ()
runServers = serverKeys >>= (go IM.empty)
 where
  go marked keys = do
    candidates <-
      fmap (take 1)
      . filterM hasEvent
      . filter (not . flip IM.member marked)
      $ keys
    if not $ null candidates
      then do
        let sid = head candidates
        runServer sid runRaft
        flushMessages sid
        go (IM.insert sid () marked) keys
      else return ()

setupServers :: [SID] -> IM.IntMap MockConfig
setupServers sids = foldl' addMockConfig IM.empty sids
 where
  addMockConfig map sid = IM.insert (unSID sid) (newMockConfig sids sid) map
