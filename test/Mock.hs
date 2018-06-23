module Mock where

import Control.Lens
import Control.Monad.State
import HaskKV.Raft
import Mock.Instances

import qualified Data.IntMap as IM

serverKeys :: MockT (IM.IntMap MockConfig) [Int]
serverKeys = MockT $ state $ \map -> (IM.keys map, map)

runServer :: Int -> MockT MockConfig a -> MockT (IM.IntMap MockConfig) a
runServer i m = do
    s <- MockT $ preuse (ix i) >>= \case
        Just s -> pure s
        _      -> error "Cannot get MockConfig"
    let (a, s') = runMockT m s
    MockT (ix i .= s')
    return a

runServers :: MockT (IM.IntMap MockConfig) ()
runServers = serverKeys >>= mapM_ (`runServer` run)
