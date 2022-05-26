{-# LANGUAGE OverloadedLabels #-}
module HaskKV.Server.Utils where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (SomeException, catch)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Binary (Binary, decodeOrFail, encode)
import Data.Conduit ((.|), runConduit, runConduitRes)
import Data.Conduit.Network
import Data.Foldable (for_, traverse_)
import Data.Streaming.Network.Internal (ClientSettings (clientPort, clientHost))
import HaskKV.Server.Types (ServerState (outgoing, messages))
import HaskKV.Utils (sinkTBQueue, sourceTBQueue)
import Optics ((%), (^.), At (at))
import System.Log.Logger (debugM)

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Conduit.List as CL
import qualified Data.IntMap as IM

retryTimeout :: Int
retryTimeout = 1000000

runServer
  :: (Binary msg, Show msg)
  => Int
  -> HostPreference
  -> IM.IntMap ClientSettings
  -> ServerState msg
  -> IO ()
runServer port host clients s = do
  forkIO $ runTCPServer (serverSettings port host) $ \appData ->
    runConduitRes
      $  appSource appData
      .| CL.mapFoldable (fmap thrd . decodeOrFail . BL.fromStrict)
      .| CL.iterM (liftIO . debugM "conduit" . ("Receiving: " ++) . show)
      .| sinkTBQueue (s ^. #messages)

  for_ (IM.assocs clients) $ \(i, settings) ->
    traverse_ (forkIO . connectClient settings) (s ^. #outgoing % at i)
 where
  thrd t = let (_, _, a) = t in a

  connectClient settings bq = do
    let connect appData =
          putStrLn "Connected to server" >> connectStream bq appData

    debugM "conduit" $ "Connecting to host " ++ show (clientHost settings)
                    ++ " and port " ++ show (clientPort settings)

    catch (runTCPClient settings connect) $ \(_ :: SomeException) -> do
      putStrLn "Error connecting to server, retrying"
      threadDelay retryTimeout
      connectClient settings bq

  connectStream bq appData =
    runConduit
      $  sourceTBQueue bq
      .| CL.iterM (liftIO . debugM "conduit" . ("Sending: " ++) . show)
      .| CL.map (B.concat . BL.toChunks . encode)
      .| appSink appData
