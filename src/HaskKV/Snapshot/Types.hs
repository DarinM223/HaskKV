{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Snapshot.Types where

import Control.Concurrent.STM (TVar)
import Data.Binary (Binary)
import GHC.Generics (Generic)
import GHC.IO.Handle (Handle)
import HaskKV.Types (FilePos, FileSize, LogIndex, LogTerm, SID)
import Optics ((^.))

import qualified Data.ByteString as B
import qualified Data.IntMap as IM

data SnapshotChunkType = FullChunk | EndChunk deriving (Show, Eq)

data SnapshotChunk = SnapshotChunk
  { chunkData :: B.ByteString
  , chunkType :: SnapshotChunkType
  , offset    :: FilePos
  , index     :: LogIndex
  , term      :: LogTerm
  } deriving (Show, Eq, Generic)

class (Binary s) => SnapshotM s m | m -> s where
  createSnapshot :: LogIndex -> LogTerm -> m ()
  writeSnapshot  :: FilePos -> B.ByteString -> LogIndex -> m ()
  saveSnapshot   :: LogIndex -> m ()
  readSnapshot   :: LogIndex -> m (Maybe s)
  hasChunk       :: SID -> m Bool
  readChunk      :: Int -> SID -> m (Maybe SnapshotChunk)
  snapshotInfo   :: m (Maybe (LogIndex, LogTerm, FileSize))

data Snapshot = Snapshot
  { file     :: Handle
  , index    :: LogIndex
  , term     :: LogTerm
  , filepath :: FilePath
  , offset   :: FilePos
  } deriving (Show, Eq, Generic)

instance Ord Snapshot where
  compare s1 s2 = compare (s1 ^. #index) (s2 ^. #index)

data Snapshots = Snapshots
  { completed :: Maybe Snapshot
  , partial   :: [Snapshot]
  , chunks    :: IM.IntMap Handle
  } deriving (Show, Eq, Generic)

data SnapshotManager = SnapshotManager
  { snapshots     :: TVar Snapshots
  , directoryPath :: FilePath
  } deriving Generic
