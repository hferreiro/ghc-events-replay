{-# LANGUAGE CPP #-}
module Main where

import GHC.RTS.Events
import GHC.RTS.Events.Analysis.Duplication

import Control.Monad
import Data.Maybe
import System.Environment
import System.IO
import System.Exit
import System.FilePath
import Text.Printf

main = getArgs >>= command

command ["--help"] = do
    putStr usage

command ["dup", name] = do
  origLog <- readLogOrDie (name <.> "replay")
  dupsLog <- readLogOrDie (name <.> "eventlog")
  let origEvents = toCapEvents origLog
      dupEvents = toCapEvents dupsLog
      events = merge origEvents dupEvents
      dupSparks = findDuplicatedSparks events
      capLost = findLostTime dupSparks events
  format capLost (sum (map snd capLost) `div` fromIntegral (length capLost))

command ["merge", name] = do
  origLog <- readLogOrDie (name <.> "replay")
  dupsLog <- readLogOrDie (name <.> "eventlog")
  let origEvents = toCapEvents origLog
      dupEvents = toCapEvents dupsLog
      events = merge origEvents dupEvents
      marked = markDuplicated events
  writeEventLogToFile (name <.> "merged") (EventLog (header dupsLog) (Data (map toBlockEvent marked)))

command ["filter", name] = do
  log <- readLogOrDie name
  let evs = events (dat log)
      filtered = filterReplay evs
  writeEventLogToFile (dropExtension name <.> "filtered") (EventLog (filterH (header log)) (Data filtered))

command _ = putStr usage >> die "Unrecognized command"

format :: [(Int,Ts)] -> Ts -> IO ()
format tss m = mapM_ (\(c, ts) -> printf "cap %d: %s" c (fmt ts)) tss >> printf "\nMean: %s\n" (fmt m)
  where
    fmt :: Ts -> String
    fmt (Single ts) = printf "%dms\n" (ms ts)
    fmt (Dual ts1 ts2) = printf "%d-%dms\n" (ms ts1) (ms ts2)

    ms n = n `div` 10^6

toCapEvents :: EventLog -> [(Maybe Int, [Event])]
toCapEvents = groupEvents . events . dat

toBlockEvent :: (Maybe Int, [TsEvent]) -> Event
toBlockEvent (c, es) = Event 0 (EventBlock 0 (fromMaybe (-1) c) (map toEvent es))
  where
    toEvent :: TsEvent -> Event
    toEvent (TsEvent ts ev) = Event (fromIntegral ts) ev

filterH :: Header -> Header
filterH (Header ets) = (Header (go ets))
  where
    go :: [EventType] -> [EventType]
    go []                         = []
    go (et@(EventType n _ _):ets)
      | n <= 59                   = et:go ets
      | otherwise                 = []

usage = unlines $ map pad strings
  where
    align = 4 + (maximum . map (length . fst) $ strings)
    pad (x, y) = zipWith const (x ++ repeat ' ') (replicate align ()) ++ y
    strings = [ ("ghc-events --help:",                     "Display this help.")
 
              , ("ghc-events dup <progname>:",             "Calculate duplicated work.")
              , ("ghc-events merge <progname>:",           "Merges the original eventlog with the replayed one with duplicates to provide to ghc-events-analyze.")
              , ("ghc-events filter <progname>:",          "Removes events used for replay.")
              ]

readLogOrDie file = do
  e <- readEventLogFromFile file
  case e of
    Left s    -> die ("Failed to parse " ++ file ++ ": " ++ s)
    Right log -> return log

#if ! MIN_VERSION_base(4,8,0)
die s = do hPutStrLn stderr s; exitWith (ExitFailure 1)
#endif
