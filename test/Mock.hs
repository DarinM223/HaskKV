module Mock where

import Control.Lens
import Control.Monad.State
import Control.Monad.Writer
import HaskKV.Raft
import Mock.Instances

import qualified Data.IntMap as IM

-- TODO(DarinM223): this should be an inspectable event
-- like a message sent or a state change.
data Event = Event

serverKeys :: MockT (IM.IntMap MockConfig) [Int]
serverKeys = MockT $ state $ \map -> (IM.keys map, map)

-- Infinitely runs through each server and builds a lazy list.
runServers :: WriterT [Event] (MockT (IM.IntMap MockConfig)) ()
runServers = do
    ids <- lift serverKeys
    let infiniteIds = cycle ids
    forM_ infiniteIds $ \i -> do
        s <- lift $ MockT $ preuse (ix i) >>= \case
            Just s -> pure s
            _      -> error "Cannot get MockConfig"
        let (_, s') = runMockT run s
        lift $ MockT (ix i .= s')

        -- TODO(DarinM223): add events to the WriterT to inspect
