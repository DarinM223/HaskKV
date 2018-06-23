module Mock where

import Control.Lens
import Control.Monad.State
import HaskKV.Raft
import Mock.Instances

import qualified Data.IntMap as IM

serverKeys :: MockT (IM.IntMap MockConfig) [Int]
serverKeys = MockT $ gets IM.keys

crashServer :: Int -> MockT (IM.IntMap MockConfig) ()
crashServer sid = MockT $ modify $ IM.delete sid

dropMessage :: Int -> MockT (IM.IntMap MockConfig) ()
dropMessage i = void $ runServer i $ MockT $ receivingMsgs %= drop 1

runServer :: Int -> MockT MockConfig a -> MockT (IM.IntMap MockConfig) (Maybe a)
runServer i m =
    MockT (preuse (ix i)) >>= \case
        Just s -> do
            let (a, s') = runMockT m s
            MockT (ix i .= s')
            return $ Just a
        _ -> return Nothing

flushMessages :: Int -> MockT (IM.IntMap MockConfig) ()
flushMessages i =
    MockT (preuse (ix i)) >>= \case
        Just s -> MockT $ do
            let messages = _sendingMsgs s
            (ix i . sendingMsgs) .= IM.empty
            forM_ (IM.assocs messages) $ \(sid, msgs) ->
                (ix sid . receivingMsgs) %= (++ msgs)
        _ -> return ()

runServers :: MockT (IM.IntMap MockConfig) ()
runServers = serverKeys >>= mapM_ (\i -> runServer i run >> flushMessages i)
