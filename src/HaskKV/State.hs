{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

module HaskKV.State where

import Control.Concurrent.STM
import Control.Monad.Reader
import HaskKV.Log (Entry, LogEntry, LogM, LogT)
import HaskKV.Serialize (Serializable)
import HaskKV.Store (Storable, StorageM (..), MemStoreT)

data RaftMessage
    = RequestVote
        { rvCandidateID :: Int
        , rvTerm        :: Int
        , rvLastLogIdx  :: Int
        , rvLastLogTerm :: Int
        }
    | AppendEntries
        { aeTerm        :: Int
        , aeLeaderId    :: Int
        , aePrevLogIdx  :: Int
        , aePrevLogTerm :: Int
        , aeEntries     :: [LogEntry]
        , aeCommitIdx   :: Int
        }
    | Response Int Bool

instance Serializable RaftMessage where
    -- TODO(DarinM223): implement this

data RaftState = RaftState
    { rsCurrTerm :: Int
    , rsVotedFor :: Int
    }

newtype ServerT k v e m a = ServerT
    { unServerT :: ReaderT (TVar RaftState) (LogT e (MemStoreT k v m)) a }
    deriving
        ( Functor, Applicative, Monad, MonadIO
        , MonadReader (TVar RaftState)
        )

instance (Monad m, Ord k, Storable v) => StorageM (ServerT k v e m) where
    type Key (ServerT k v e m) = k
    type Value (ServerT k v e m) = v

    -- TODO(DarinM223): implement this

instance (Entry e) => LogM e (ServerT k v e m) where
    -- TODO(DarinM223): implement this
