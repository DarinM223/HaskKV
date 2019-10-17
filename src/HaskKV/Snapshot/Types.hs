module HaskKV.Snapshot.Types where

import Control.Concurrent.STM
import Control.Lens
import Control.Monad.Reader
import Data.Binary (Binary)
import Data.Generics.Product.Fields
import GHC.IO.Handle
import GHC.Generics
import HaskKV.Types

import qualified Data.ByteString as B
import qualified Data.IntMap as IM

class (Binary s) => HasSnapshotType s (m :: * -> *) | m -> s

data SnapshotChunkType = FullChunk | EndChunk deriving (Show, Eq)

data SnapshotChunk = SnapshotChunk
  { _data   :: B.ByteString
  , _type   :: SnapshotChunkType
  , _offset :: FilePos
  , _index  :: LogIndex
  , _term   :: LogTerm
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
  { _file     :: Handle
  , _index    :: LogIndex
  , _term     :: LogTerm
  , _filepath :: FilePath
  , _offset   :: FilePos
  } deriving (Show, Eq, Generic)

instance Ord Snapshot where
  compare s1 s2 = compare (s1^.field' @"_index") (s2^.field' @"_index")

data Snapshots = Snapshots
  { _completed :: Maybe Snapshot
  , _partial   :: [Snapshot]
  , _chunks    :: IM.IntMap Handle
  } deriving (Show, Eq)

data SnapshotManager = SnapshotManager
  { _snapshots     :: TVar Snapshots
  , _directoryPath :: FilePath
  }

class HasSnapshotManager r where
  snapshotManagerL :: Lens' r SnapshotManager

newtype SnapshotT m a = SnapshotT { unSnapshotT :: m a }
  deriving (Functor, Applicative, Monad, MonadIO, MonadReader r)
