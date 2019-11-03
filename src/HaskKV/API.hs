module HaskKV.API
  ( api
  , server
  )
where

import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM (newEmptyTMVarIO)
import Control.Monad.Except
import Control.Monad.Reader
import Data.Aeson (FromJSON, ToJSON)
import Data.Proxy (Proxy(Proxy))
import HaskKV.Constr (Constr, run)
import HaskKV.Log.Entry
import HaskKV.Log.Temp (waitApplyEntry)
import HaskKV.Monad
import HaskKV.Raft.State
import HaskKV.Server.All
import HaskKV.Store.Types
import Optics
import Servant.API
import Servant.Server

type StoreAPI k v
  =    "get" :> Capture "key" k :> Get '[JSON] (Maybe v)
  :<|> "set" :> Capture "key" k :> ReqBody '[JSON] v :> Post '[JSON] ()
  :<|> "delete" :> Capture "key" k :> Delete '[JSON] ()

type MyHandler msg k v e a = ExceptT ServantErr (App msg k v e) a

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
  :: (KeyClass k, ValueClass v, FromHttpApiData k, FromJSON v, ToJSON v)
  => AppConfig msg k v (LogEntry k v)
  -> Server (StoreAPI k v)
server config = hoistServer api (convertApp config) server
  where server = getRoute :<|> setRoute :<|> deleteRoute

applyEntryData :: LogEntryData k v -> App msg k v (LogEntry k v) ()
applyEntryData entryData = ask >>= \config -> liftIO $ do
  completed <- Completed . Just <$> newEmptyTMVarIO
  let entry = LogEntry { logEntryTerm      = 0
                       , logEntryIndex     = 0
                       , logEntryData      = entryData
                       , logEntryCompleted = completed
                       }
  f <- async $ run config $ waitApplyEntry entry
  inject HeartbeatTimeout $ config ^. serverStateL
  wait f
