module HaskKV.Snapshot.Utils where

import Control.Concurrent.STM
import Control.Exception
import Data.List
import Data.Maybe
import GHC.IO.Handle
import HaskKV.Snapshot.Types
import HaskKV.Types
import System.Directory
import System.FilePath
import System.IO
import Text.Read (readMaybe)

import qualified Data.IntMap as IM

newSnapshotManager :: Maybe FilePath -> IO SnapshotManager
newSnapshotManager path = do
    let directoryPath = fromMaybe "" path
    snapshots <- loadSnapshots directoryPath
    return SnapshotManager
        { _snapshots = snapshots, _directoryPath = directoryPath }

closeSnapshotManager :: SnapshotManager -> IO ()
closeSnapshotManager manager = do
    snapshots <- readTVarIO $ _snapshots manager
    mapM_ (hClose . _file) (_completed snapshots)
    mapM_ (hClose . _file) (_partial snapshots)

loadSnapshots :: FilePath -> IO (TVar Snapshots)
loadSnapshots path = do
    files <- catch (getDirectoryContents path) $ \(_ :: SomeException) ->
        return []
    partial <- mapM (toPartial . (path </>)) . filter isPartial $ files
    completed <- mapM (toCompleted . (path </>)) . filter isCompleted $ files
    newTVarIO Snapshots
        { _completed = listToMaybe $ catMaybes completed
        , _partial   = catMaybes partial
        , _chunks    = IM.empty
        }
  where
    toSnapshot mode path = case fileSnapInfo (fileBase path) of
        Just (index, term) -> do
            handle <- openFile path mode
            fileSize <- hFileSize handle
            let offset = if fileSize > 0 then fromIntegral fileSize else 0
            return $ Just Snapshot
                { _file     = handle
                , _index    = index
                , _term     = term
                , _filepath = path
                , _offset   = offset
                }
        Nothing -> return Nothing
    toPartial = toSnapshot AppendMode
    toCompleted = toSnapshot ReadMode

    isPartial = (== ".partial.snap") . fileExt
    isCompleted = (== ".completed.snap") . fileExt

partialFilename :: LogIndex -> LogTerm -> String
partialFilename i t = (show i) ++ "_" ++ (show t) ++ ".partial.snap"

completedFilename :: LogIndex -> LogTerm -> String
completedFilename i t = (show i) ++ "_" ++ (show t) ++ ".completed.snap"

fileBase :: FilePath -> String
fileBase path
    | hasExtension path = fileBase $ takeBaseName path
    | otherwise         = path

fileExt :: FilePath -> String
fileExt = go ""
  where
    go fullExt path = case splitExtension path of
        (_, "")           -> fullExt
        (incomplete, ext) -> go (ext ++ fullExt) incomplete

getPos :: HandlePosn -> FilePos
getPos (HandlePosn _ pos) = FilePos $ fromIntegral pos

fileSnapInfo :: String -> Maybe (LogIndex, LogTerm)
fileSnapInfo s = (,) <$> index <*> term
  where
    i = findIndex (== '_') s
    index = fmap LogIndex . readMaybe =<< flip take s <$> i
    term = fmap LogTerm . readMaybe =<< flip drop s . (+ 1) <$> i
