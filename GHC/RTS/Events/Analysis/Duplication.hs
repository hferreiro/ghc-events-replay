{-# LANGUAGE ViewPatterns #-}
module GHC.RTS.Events.Analysis.Duplication (
    findDuplicatedSparks
  , findLostTime
  , markDuplicated
  , merge
  , filterReplay
  , TsEvent(..)
  , Ts(..)
  ) where

import Data.Typeable

import Data.Function
import Data.List
import Data.Maybe
import Data.Tuple
import Data.Word
import qualified Data.Map.Strict as M
import GHC.RTS.Events
import Text.Printf

import Debug.Trace


type SparkId = Word64
type SparkMap = M.Map Pointer (SparkId, [ThreadId])
  -- (current thread, map from spark ptr to id and threads that updated it)

data Dup a = Suspended a ThreadId
           | Duped a [ThreadId]
  deriving (Show)

data Ts = Single Timestamp
        | Dual Timestamp Timestamp
  deriving (Eq, Ord, Show)

data TsEvent = TsEvent {
                 ts :: Ts
               , ev ::EventInfo
               }
instance Num Ts where
  Single ts1       + Single ts2       = Single (ts1+ts2)
  Single ts        + (Dual ts1 ts2)   = Dual (ts+ts1) (ts+ts2)
  (Dual ts1 ts2)   + (Dual ts1' ts2') = Dual (ts1+ts1') (ts2+ts2')
  ts1              + ts2              = ts2 + ts1

  Single ts1       * Single ts2       = Single (ts1*ts2)
  Single ts        * (Dual ts1 ts2)   = Dual (ts*ts1) (ts*ts2)
  (Dual ts1 ts2)   * (Dual ts1' ts2') = Dual (ts1*ts1') (ts2*ts2')
  ts1              * ts2              = ts2 * ts1

  signum (Single ts) = Single (signum ts)
  signum (Dual ts1 ts2) = Dual (signum ts1) (signum ts2)

  abs (Single ts) = Single (abs ts)
  abs (Dual ts1 ts2) = Dual (abs ts1) (abs ts2)

  fromInteger n = Single (fromInteger n)

  negate (Single ts) = Single (negate ts)
  negate (Dual ts1 ts2) = Dual (negate ts1) (negate ts2)

instance Real Ts where
  toRational ts = toRational (fromIntegral ts)
instance Enum Ts where
  toEnum n = Single (fromIntegral n)
  fromEnum ts = fromIntegral ts
instance Integral Ts where
  toInteger (Single ts1) = toInteger ts1
  toInteger (Dual ts1 ts2) = round (fromIntegral (ts1 + ts2) / 2.0)

  quotRem (Single ts1) (Single ts2) = (Single (ts1 `quot` ts2), Single (ts1 `rem` ts2))
  quotRem (Dual ts1 ts2) (Single ts) = (Dual (ts1 `quot` ts) (ts2 `quot` ts),
                                        Dual (ts1 `rem` ts) (ts2 `rem` ts))
  quotRem (Dual ts1 ts2) (Dual ts1' ts2') = (Dual (ts1 `quot` ts1') (ts2 `quot` ts2'),
                                             Dual (ts1 `rem` ts1') (ts2 `rem` ts2'))
  quotRem ts1 ts2 = quotRem ts2 ts1

findDuplicatedSparks :: [(Maybe Int, [TsEvent])] -> [Dup SparkId]
findDuplicatedSparks ces = check (allButFirst dups) ++ sus
  where
    (dups, sus) = partition isDup all
    isDup (Duped _ _) = True
    isDup _           = False

    -- make sure no duplicated spark is suspended
    check :: [Dup SparkId] -> [Dup SparkId]
    check ds = go ds
      where
        ss = map (\(Suspended id _) -> id) sus

        go []                    = []
        go (d@(Duped id [t]):ds) 
          | id `elem` ss         = error (printf "findDuplicatedSparks: suspended %d is duplicated in thread %d" id t)
          | otherwise            = d:go ds

    all = go evinfos

    evinfos = concat [map ev es | (Just _, es) <- ces]

    go :: [EventInfo] -> [Dup SparkId]
    go []               = []
    go (RunThread t:es) = inThread es
      where
        inThread ((flip dupEvent t -> Just dup):es) = dup:inThread es
        inThread (StopThread _ _:es)         = go es
        inThread (_:es)                      = inThread es
    go (e:es)                                = go es

    dupEvent :: EventInfo -> ThreadId -> Maybe (Dup SparkId)
    dupEvent (CapValue DupSpark id)          t = Just (Duped id [t])
    dupEvent (CapValue SuspendComputation p) t = flip Suspended t `fmap` id
      where
        id = M.lookup (Pointer p) ptrMap
    dupEvent _                               _ = Nothing

    -- for each spark pointer, find its id
    ptrMap :: M.Map Pointer SparkId
    ptrMap = buildPtrMap evinfos

    -- for each spark, find which threads entered it
    used :: M.Map SparkId [ThreadId]
    used = buildUsedMap evinfos

    -- for each duplicated spark, find the real duplicates: all sparks entered
    -- after the first one
    allButFirst :: [Dup SparkId] -> [Dup SparkId]
    allButFirst dups = go (map ev (sortGroups ces)) []
      where
        sortGroups groups = mergesort' (compare `on` ts) $
                              [ [ e | e <- es ] | (_, es) <- groups ]

        mergesort' :: (a -> a -> Ordering) -> [[a]] -> [a]
        mergesort' _   [] = []
        mergesort' _   [xs] = xs
        mergesort' cmp xss = mergesort' cmp (merge_pairs cmp xss)

        merge_pairs :: (a -> a -> Ordering) -> [[a]] -> [[a]]
        merge_pairs _   [] = []
        merge_pairs _   [xs] = [xs]
        merge_pairs cmp (xs:ys:xss) = merge cmp xs ys : merge_pairs cmp xss

        merge :: (a -> a -> Ordering) -> [a] -> [a] -> [a]
        merge _   [] ys = ys
        merge _   xs [] = xs
        merge cmp (x:xs) (y:ys)
         = case x `cmp` y of
                 GT -> y : merge cmp (x:xs)   ys
                 _  -> x : merge cmp    xs (y:ys)

        (ids, threads) = unzip (map toPair dups)
        toPair (Duped id [t]) = (id, t)

        go :: [EventInfo] -> [SparkId] -> [Dup SparkId]
        go []               ss = []
        go (RunThread t:es) ss = inThread es ss
          where
            inThread (CapValue EnterSpark id:es) ss
              -- save first time the spark is entered
              | id `elem` ids && not (id `elem` ss) = inThread es (id:ss)
              -- return the next ones
              | id `elem` ids                       = Duped id [t]:inThread es ss
            inThread (StopThread _ _:es)         ss = go es ss
            inThread (_:es)                      ss = inThread es ss
        go (_:es)                                ss = go es ss


findLostTime :: [Dup SparkId] -> [(Maybe Int, [TsEvent])] -> [(Int, Ts)]
findLostTime ids ces = snd (findLostTime_ ids ces)

findLostTime_ :: [Dup SparkId] -> [(Maybe Int, [TsEvent])] -> ([(SparkId, [ThreadId])], [(Int, Ts)])
findLostTime_ ids ces = (all, [(c, wasted es) | (Just c, es) <- ces])
  where
    (dups_, sus_) = partition isDup ids
    isDup (Duped _ _) = True
    isDup _           = False

    dups = [(id, ts) | Duped id ts <- dups_]
    sus = [(id, [t]) | Suspended id t <- sus_] -- if dup from suspended, dup both

    evinfos :: [EventInfo]
    evinfos = concat [map ev es | (Just _, es) <- ces]

    all = clean (extend evinfos (dups ++ sus))

    wasted :: [TsEvent] -> Ts
    wasted es = go es M.empty (fromInteger 0)
      where
        threads = concatMap snd all

        go :: [TsEvent] -> M.Map ThreadId (SparkId, Ts) -> Ts -> Ts
        go []                           tm _
          | not (M.null tm)                     = error (printf "wasted: missed an update event %s" (show tm))
        go []                           _  ts   = ts
        go ((TsEvent _ (RunThread t)):es) tm ts
          | t `elem` threads                    = inThread (M.lookup t tm) es tm ts
          where
            inThread Nothing         ((TsEvent tms (CapValue EnterSpark id)):es)   tm ts
              | maybe False (t `elem`) (lookup id all)                                   = inThread (Just (id,tms)) es (M.insert t (id,tms) tm) ts
            inThread (Just (id,_))   ((TsEvent _ (CapValue EnterSpark id')):es)    tm ts
              | maybe False (t `elem`) (lookup id' all)                                  = error (printf "Nested id %d while calculating for %d" id' id)
            inThread (Just (id,tms)) ((TsEvent tms' (updateEvent -> Just id')):es) tm ts
              | id' == id                                                                = inThread Nothing es (M.delete t tm) (ts + (tms' - tms))
            inThread _               ((TsEvent _ (StopThread _ _)):es)             tm ts = go es tm ts
            inThread s               (_:es)                                        tm ts = inThread s es tm ts
        go (_:es)                         tm ts = go es tm ts
        -- TODO: shutdown

    -- remove nested duplicated sparks
    clean :: [(SparkId, [ThreadId])] -> [(SparkId, [ThreadId])]
    clean [] = []
    clean ss = M.toList $ M.differenceWith ((notEmpty .) . (\\)) sm (go evinfos M.empty M.empty)
      where
        notEmpty [] = Nothing
        notEmpty ts = Just ts
                         
        sm = M.fromList ss
        threads = concatMap snd ss

        go :: [EventInfo] -> M.Map ThreadId SparkId -> M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId]
        go []               tm _ 
          | not (M.null tm)       = error (printf "clean: missed an update event: %s" (show tm))
        go []               _  ts = ts
        go (RunThread t:es) tm ts
          | t `elem` threads      = inThread (M.lookup t tm) es tm ts
          where
            inThread :: Maybe SparkId -> [EventInfo] -> M.Map ThreadId SparkId -> M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId]
            inThread Nothing   (CapValue EnterSpark id:es)    tm ts
              | maybe False (t `elem`) (M.lookup id sm)             = inThread (Just id) es (M.insert t id tm) ts
            inThread (Just id) (CapValue EnterSpark id':es)   tm ts = inThread (Just id) es tm (M.insertWith (++) id' [t] ts)
            inThread (Just id) ((updateEvent -> Just id'):es) tm ts
              | id' == id                                           = inThread Nothing es (M.delete t tm) ts
            inThread _         (StopThread _ _:es)            tm ts = go es tm ts
            inThread s         (_:es)                         tm ts = inThread s es tm ts
        go (_:es)           tm ts = go es tm ts
        
    -- for each (id, thread), collect created sparks that are evaluated (on
    -- other threads)
    extend :: [EventInfo] -> [(SparkId, [ThreadId])] -> [(SparkId, [ThreadId])]
    extend _   [] = []
    extend evs ss = M.toList (extend' sm sm)
      where
        sm = M.fromList ss

        extend' :: M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId]
        extend' sm acc
          | M.null sm  = acc
        extend' sm acc = extend' new (M.unionWith (++) new acc)
          where
            new = go evs M.empty M.empty

            threads = concatMap snd (M.toList sm)

            go :: [EventInfo] -> M.Map ThreadId SparkId -> M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId]
            go (RunThread t:es)   tm sm'
              | t `elem` threads         = inThread (M.lookup t tm) es tm sm'
              where
                inThread Nothing   (CapValue EnterSpark id:es)    tm sm'
                  | maybe False (t `elem`) (M.lookup id sm)              = inThread (Just id) es (M.insert t id tm) sm'
                -- add new sparks that are used elsewhere
                inThread (Just id) (CapValue CreateSpark id':es)  tm sm'
                  | not (null ts)                                        = inThread (Just id) es tm (M.insertWith undefined id' ts sm')
                  where
                    ts  = maybe all (all \\) (M.lookup id' acc)
                    all = M.findWithDefault [] id' used
                -- remove updated sparks within the current spark (saves time
                -- later in clean)
                inThread (Just id) (CapValue EnterSpark id':es)   tm sm' = inThread (Just id) es tm (M.filter (not . null) (M.adjust (delete t) id' sm'))
                inThread (Just id) ((updateEvent -> Just id'):es) tm sm'
                  | id' == id                                            = inThread Nothing es (M.delete t tm) sm'
                inThread _         (StopThread _ _:es)            tm sm' = go es tm sm'
                inThread s         (_:es)                         tm sm' = inThread s es tm sm'
            -- additional update events for cancelled threads when shutting
            -- down
            go (RequestSeqGC:es)  tm sm' = shutdown es tm sm'
            go (CapValue GC 0:es) tm sm' = shutdown es tm sm'
            go (_:es)             tm sm' = go es tm sm'

            shutdown :: [EventInfo] -> M.Map ThreadId SparkId -> M.Map SparkId [ThreadId] -> M.Map SparkId [ThreadId]
            shutdown ((updateEvent -> Just id):es) tm sm' = case lookup id (map swap (M.toList tm)) of
                                                              Just t -> shutdown es (M.delete t tm) sm'
                                                              Nothing -> shutdown es tm sm'
            shutdown (_:es)                        tm sm' = shutdown es tm sm'
            shutdown []                            tm _
              | not (M.null tm)                           = error (printf "extend: missed an update event: %s" (show tm))
            shutdown []                            _  sm' = sm'

    updateEvent :: EventInfo -> Maybe SparkId
    updateEvent (CapValue WhnfSpark id)         = Just id
    updateEvent (CapValue SuspendComputation p) = M.lookup (Pointer p) ptrMap
    updateEvent _                               = Nothing

    -- for each spark pointer, find its id
    ptrMap :: M.Map Pointer SparkId
    ptrMap = buildPtrMap evinfos

    -- for each spark, find which threads entered it
    used :: M.Map SparkId [ThreadId]
    used = buildUsedMap evinfos

markDuplicated :: [(Maybe Int, [TsEvent])] -> [(Maybe Int, [TsEvent])]
markDuplicated ces = map insertMarks ces
  where
    dups = fst (findLostTime_ (findDuplicatedSparks ces) ces)

    insertMarks :: (Maybe Int, [TsEvent]) -> (Maybe Int, [TsEvent])
    insertMarks ces@(Nothing, _) = ces
    insertMarks (Just cap, es)   = (Just cap, go es M.empty)
      where
        threads = concatMap snd dups

        go :: [TsEvent] -> M.Map ThreadId SparkId -> [TsEvent]
        go []                                tm
          | not (M.null tm)                     = error (printf "wasted: missed an update event %s" (show tm))
        go []                                _  = []
        go (e@(TsEvent ts (RunThread t)):es) tm
          | t `elem` threads                    = case M.lookup t tm of
                                                    s@(Just _) -> e:start cap ts:inThread s es tm
                                                    Nothing -> e:inThread Nothing es tm
          where
            inThread Nothing   (e@(TsEvent ts (CapValue EnterSpark id)):es)  tm
              | maybe False (t `elem`) (lookup id dups)                         = start cap ts:e:inThread (Just id) es (M.insert t id tm)
            inThread (Just id) (e@(TsEvent ts (updateEvent -> Just id')):es) tm
              | id' == id                                                       = e:stop cap ts:inThread Nothing es (M.delete t tm)
            inThread (Just _)  (e@(TsEvent ts (StopThread _ _)):es)          tm = stop cap ts:e:go es tm
            inThread _         (e@(TsEvent _ (StopThread _ _)):es)           tm = e:go es tm
            inThread s         (e:es)                                        tm = e:inThread s es tm
        go (e:es)                            tm = e:go es tm

        start :: Int -> Ts -> TsEvent
        start cap ts = TsEvent ts (UserMessage ("START duplicated in cap " ++ show cap))

        stop :: Int -> Ts -> TsEvent
        stop cap ts = TsEvent ts (UserMessage ("STOP duplicated in cap " ++ show cap))

    updateEvent :: EventInfo -> Maybe SparkId
    updateEvent (CapValue WhnfSpark id)         = Just id
    updateEvent (CapValue SuspendComputation p) = M.lookup (Pointer p) ptrMap
    updateEvent _                               = Nothing

    -- for each spark pointer, find its id
    ptrMap :: M.Map Pointer SparkId
    ptrMap = buildPtrMap evinfos

    evinfos = concat [map ev es | (Just _, es) <- ces]

merge :: [(Maybe Int, [Event])] -> [(Maybe Int, [Event])] -> [(Maybe Int, [TsEvent])]
merge orig dup = zipWith mergeC orig dup
  where
    mergeC (Just c, es)  (Just c', es')
      | c == c'            = (Just c, mergeE 0 es es')
      | otherwise          = error (printf "Trying to merge events from cap %d with cap %d" c c')
    mergeC (Nothing, es) _ = (Nothing, map (\(Event t ev) -> TsEvent (fromIntegral t) ev) es)

    mergeE :: Timestamp -> [Event] -> [Event] -> [TsEvent]
    mergeE _ []                                []                             = []
    mergeE t es@(Event t' _:_)                 (Event _ e'@(CapValue EnterSpark _):es') = TsEvent (Dual t' t) e':mergeE t es es'
    -- WhnfSpark and DupSpark take the timestamp of the following ThunkUpdate
    mergeE t es@(Event t0 (ThunkUpdate _ _):_) (Event _ e'@(CapValue DupSpark _):es')   = TsEvent (Single t0) e':mergeE t0 es es'
    mergeE t es@(Event t0 (ThunkUpdate _ _):_) (Event _ e'@(CapValue WhnfSpark _):es')  = TsEvent (Single t0) e':mergeE t0 es es'
    -- WhnfSpark created for last update overwriting a blocking queue,
    -- ThunkUpdate does not happen in original run (not needed)
    mergeE t es@(Event t' _:_)                 (Event _ e'@(CapValue WhnfSpark _):es')  = TsEvent (Dual t' t) e':mergeE t es es'
    --mergeE _ (Event _ e:_)                     (Event _ e':_)                 | e /= e' = error (printf "merge: events %s and %s do not match" (show e) (show e'))
    mergeE _ (e@(Event t ev):es)               (_:es')                                  = TsEvent (Single t) ev:mergeE t es es'
    mergeE _ es                                es'                                      = error (printf "merge: size mismatch: %d:%s and %d:%s" (length es) (show $ es) (length es') (show $ es'))

filterReplay :: [Event] -> [Event]
filterReplay es = [Event t (filterB ev) | Event t ev <- es, not (isReplayE ev)]
  where
    filterB :: EventInfo -> EventInfo
    filterB (EventBlock t c es) = EventBlock t c (filterReplay es)
    filterB e                   = e

    isReplayE :: EventInfo -> Bool
    isReplayE (CapAlloc {}) = True
    isReplayE (CapValue {}) = True
    isReplayE (TaskAcquireCap {}) = True
    isReplayE (TaskReleaseCap {}) = True
    isReplayE (TaskReturnCap {}) = True
    isReplayE (ThunkUpdate {}) = True
    isReplayE (Startup 0) = True
    isReplayE _ = False

--
buildPtrMap :: [EventInfo ] -> M.Map Pointer SparkId
buildPtrMap es = go es M.empty
  where
    go :: [EventInfo] -> M.Map Pointer SparkId -> M.Map Pointer SparkId
    go []                    m = m
    go (ThunkUpdate id p:es) m 
      | id /= 0                = go es (M.insert p id m)
    go (_:es)                m = go es m


buildUsedMap :: [EventInfo] -> M.Map SparkId [ThreadId]
buildUsedMap es = go es M.empty
  where
    go []               m = m
    go (RunThread t:es) m = inThread es m
      where
        inThread (StopThread _ _:es)            m = go es m
        inThread ((CapValue EnterSpark  id):es) m = inThread es (M.insertWith (++) id [t] m)
        inThread (_:es)                         m = inThread es m
    go (_:es)           m = go es m
