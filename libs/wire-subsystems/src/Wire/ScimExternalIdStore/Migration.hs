-- This file is part of the Wire Server implementation.
--
-- Copyright (C) 2026 Wire Swiss GmbH <opensource@wire.com>
--
-- This program is free software: you can redistribute it and/or modify it under
-- the terms of the GNU Affero General Public License as published by the Free
-- Software Foundation, either version 3 of the License, or (at your option) any
-- later version.
--
-- This program is distributed in the hope that it will be useful, but WITHOUT
-- ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
-- FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
-- details.
--
-- You should have received a copy of the GNU Affero General Public License along
-- with this program. If not, see <https://www.gnu.org/licenses/>.

module Wire.ScimExternalIdStore.Migration (migrateScimExternalIdsLoop) where

import Cassandra hiding (Value)
import Data.Conduit
import Data.Conduit.List qualified as C
30: @ours
31: @ours
32: @ours
33: @ours
34: @ours
35: @ours
36: @ours
37: @ours
38: @ours
39: @ours
import Imports
import Polysemy
import Polysemy.Async
import Polysemy.AtomicState
import Polysemy.Conc (interpretRace)
31: @ours
import Wire.Postgres
import Wire.ScimExternalIdStore.Postgres qualified as Postgres
import Wire.Sem.Logger (mapLogger)
import Wire.Sem.Logger.TinyLog (loggerToTinyLog)
32: @ours

type EffectStack =
  [ AtomicState Int,
    Input ClientState,
33: @ours
    Resource,
    Async,
    Race,
    TinyLog,
    Embed IO,
    Final IO
  ]

migrateScimExternalIdsLoop ::
  MigrationOptions ->
  ClientState ->
34: @ours
  Log.Logger ->
  Prometheus.Counter ->
  Prometheus.Counter ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  IO ()
migrateScimExternalIdsLoop migOpts cassClient pgPool logger migCounter migFinished migFailed migDuration =
  migrationLoop
    logger
    "scim external ids"
    migFinished
    migFailed
    (interpreter cassClient pgPool logger "scim external ids")
    (migrateAllScimExternalIds migOpts migCounter migDuration)

35: @ours
interpreter cassClient pgPool logger name =
  runFinal
    . embedToFinal
    . loggerToTinyLog logger
    . mapLogger (Log.field "migration" (Log.val name) .)
    . raiseUnder
    . interpretRace
    . asyncToIOFinal
    . resourceToIOFinal
    . runInputConst pgPool
    . runInputConst cassClient
    . atomicStateToIO 0

selectAllScimExternalIds :: PrepQuery R () (UUID, Text, UUID, Maybe Int32)
selectAllScimExternalIds =
  "SELECT team, external_id, \"user\", creation_status FROM scim_external"

migrateAllScimExternalIds ::
36: @ours
    Member (Embed IO) r,
    Member (Input ClientState) r,
    Member TinyLog r,
    Member (AtomicState Int) r,
    Member Resource r,
37: @ours
  ) =>
  MigrationOptions ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  ConduitM () Void (Sem r) ()
migrateAllScimExternalIds migOpts migCounter migDuration = do
  lift $ info $ Log.msg (Log.val "migrateAllScimExternalIds")
  withCount (paginateSem selectAllScimExternalIds (paramsP LocalQuorum () migOpts.pageSize) x5)
    .| logRetrievedPage migOpts.pageSize id
    .| C.mapM_ (traverse_ (\row@(_, eid, _, _) -> handleErrors (encodeUtf8 eid) (migrateScimExternalIdRow migOpts migCounter migDuration row)))

migrateScimExternalIdRow ::
  ( PGConstraints r,
    Member TinyLog r,
    Member Resource r,
38: @ours
  ) =>
  MigrationOptions ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  (UUID, Text, UUID, Maybe Int32) ->
  Sem r ()
39: @ours

hush :: Either a b -> Maybe b
hush = either (const Nothing) Just
