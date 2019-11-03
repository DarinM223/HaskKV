{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module HaskKV.Snapshot.Types where

import Control.Concurrent.STM
import Control.Monad.Reader
import Data.Binary (Binary)
import GHC.IO.Handle
import HaskKV.Types
import Optics

import qualified Data.ByteString as B
import qualified Data.IntMap as IM

class (Binary s) => HasSnapshotType s (m :: * -> *) | m -> s

data SnapshotChunkType = FullChunk | EndChunk deriving (Show, Eq)

data SnapshotChunk = SnapshotChunk
  { snapshotChunkData   :: B.ByteString
  , snapshotChunkType   :: SnapshotChunkType
  , snapshotChunkOffset :: FilePos
  , snapshotChunkIndex  :: LogIndex
  , snapshotChunkTerm   :: LogTerm
  } deriving (Show, Eq)
makeFieldLabels ''SnapshotChunk

class (Binary s) => SnapshotM s m | m -> s where
  createSnapshot :: LogIndex -> LogTerm -> m ()
  writeSnapshot  :: FilePos -> B.ByteString -> LogIndex -> m ()
  saveSnapshot   :: LogIndex -> m ()
  readSnapshot   :: LogIndex -> m (Maybe s)
  hasChunk       :: SID -> m Bool
  readChunk      :: Int -> SID -> m (Maybe SnapshotChunk)
  snapshotInfo   :: m (Maybe (LogIndex, LogTerm, FileSize))

data Snapshot = Snapshot
  { snapshotFile     :: Handle
  , snapshotIndex    :: LogIndex
  , snapshotTerm     :: LogTerm
  , snapshotFilepath :: FilePath
  , snapshotOffset   :: FilePos
  } deriving (Show, Eq)
makeFieldLabels ''Snapshot

instance Ord Snapshot where
  compare s1 s2 = compare (s1 ^. #index) (s2 ^. #index)

data Snapshots = Snapshots
  { snapshotsCompleted :: Maybe Snapshot
  , snapshotsPartial   :: [Snapshot]
  , snapshotsChunks    :: IM.IntMap Handle
  } deriving (Show, Eq)
makeFieldLabels ''Snapshots

data SnapshotManager = SnapshotManager
  { snapshotManagerSnapshots     :: TVar Snapshots
  , snapshotManagerDirectoryPath :: FilePath
  }
makeFieldLabels ''SnapshotManager

class HasSnapshotManager r where
  snapshotManagerL :: Lens' r SnapshotManager

newtype SnapshotT m a = SnapshotT { unSnapshotT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)
