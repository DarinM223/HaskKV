module HaskKV.TCPConn where

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Concurrent.STM
import Control.Monad.IO.Class
import Data.Binary
import Data.Function (fix)
import Data.Int
import HaskKV.Log (Entry, LogEntry)
import HaskKV.Raft (Params, RaftMessage, RaftState, execRaftTParams)
import HaskKV.Store (Store, Storable)
import Network.Socket

import qualified HaskKV.Server as S
import qualified Data.ByteString.Lazy as BS
import qualified Network.Socket.ByteString.Lazy as NBS

type MsgLen = Word16

msgLenLen :: Int64
msgLenLen = 2

isEOT :: MsgLen -> Bool
isEOT = (== 0)

runServer :: (Ord k, Storable v, Entry e) => Params k v e -> IO ()
runServer params = do
    sock <- socket AF_INET Stream 0
    setSocketOption sock ReuseAddr 1
    bind sock (SockAddrInet 4242 iNADDR_ANY)
    listen sock 2

    chan <- newChan

    -- TODO(DarinM223): read config and fork threads to handle every worker
    -- in the config.

    mainLoop sock chan params

mainLoop :: (Ord k, Storable v, Entry e)
         => Socket
         -> Chan (RaftMessage e)
         -> Params k v e
         -> IO ()
mainLoop sock chan params = do
    conn <- accept sock
    commLine <- liftIO $ dupChan chan
    -- TODO(DarinM223): use copy of params with channel as commLine
    -- forkIO $ execRaftTParams runConn params
    mainLoop sock chan params

runConn :: (S.ServerM msg m) => m ()
runConn = do
    msgMaybe <- S.recv
    mapM_ S.send msgMaybe
    runConn
