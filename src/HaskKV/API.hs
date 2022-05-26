{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLabels #-}
module HaskKV.API
  ( api
  , convertApp
  , server
  )
where

import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM (newEmptyTMVarIO)
import Control.Monad.Except
import Control.Monad.Reader (MonadReader (ask))
import Data.Proxy (Proxy(Proxy))
import HaskKV.Constr (Constr, run)
import HaskKV.Log.Entry
import HaskKV.Log.Temp (waitApplyEntry)
import HaskKV.Monad (App, AppConfig, runApp)
import HaskKV.Raft.State (StateType (Leader))
import HaskKV.Server.Instances (inject)
import HaskKV.Server.Types (ServerEvent (HeartbeatTimeout), HasServerState (serverStateL))
import HaskKV.Store.Types (KeyClass, ValueClass, StorageM (getValue))
import Optics ((^.), use)
import Servant.API hiding (inject)
import Servant.Server

type StoreAPI k v
  =    "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
  :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
  :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

type MyHandler msg k v e = ExceptT ServerError (App msg k v e)

api :: Proxy (StoreAPI k v)
api = Proxy

getRoute :: (Constr k v e) => k -> MyHandler msg k v e (Maybe v)
getRoute key = checkLeader $ getValue key

setRoute :: k -> v -> MyHandler msg k v (LogEntry k v) ()
setRoute key value = checkLeader $ applyEntryData $ Change (TID 0) key value

deleteRoute :: k -> MyHandler msg k v (LogEntry k v) ()
deleteRoute key = checkLeader $ applyEntryData $ Delete (TID 0) key

checkLeader :: App msg k v e r -> MyHandler msg k v e r
checkLeader handler = lift (use #stateType) >>= \case
  Leader _  -> lift handler
  _         -> throwError err404

convertApp :: AppConfig msg k v e -> MyHandler msg k v e a -> Handler a
convertApp config = Handler . ExceptT . flip runApp config . runExceptT

server
  :: (KeyClass k, ValueClass v)
  => ServerT (StoreAPI k v) (MyHandler msg k v (LogEntry k v))
server = getRoute :<|> setRoute :<|> deleteRoute

applyEntryData :: LogEntryData k v -> App msg k v (LogEntry k v) ()
applyEntryData entryData = ask >>= \config -> liftIO $ do
  completed <- Completed . Just <$> newEmptyTMVarIO
  let entry = LogEntry { term      = 0
                       , index     = 0
                       , entryData = entryData
                       , completed = completed
                       }
  f <- async $ run config $ waitApplyEntry entry
  inject HeartbeatTimeout $ config ^. serverStateL
  wait f
