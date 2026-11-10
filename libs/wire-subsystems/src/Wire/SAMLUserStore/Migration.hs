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

module Wire.SAMLUserStore.Migration (migrateSAMLUsersLoop) where

import Cassandra hiding (Value)
import Data.Conduit
import Data.Conduit.Combinators qualified as C
import Data.IORef qualified as IORef
import Data.Id
import Data.Text.Encoding (encodeUtf8)
import Data.Time
import Data.UUID qualified as UUID
import Hasql.Pool.Extended qualified as HasqlPoolExt
import Imports
import Polysemy
import Polysemy.Async
import Polysemy.AtomicState
import Polysemy.Conc (interpretRace)
import Polysemy.Conc qualified as Conc
import Polysemy.Conc.Effect.Race hiding (Timeout)
import Polysemy.Input
import Polysemy.Resource (Resource, bracket, resourceToIOFinal)
import Polysemy.TinyLog
import Prometheus qualified
import SAML2.WebSSO qualified as SAML
import System.Logger qualified as Log
import UnliftIO qualified
import {- instance Cql NormalizedUNameID -} Wire.API.User.Saml (NormalizedUNameID, normalizeQualifiedNameId)
import {- instance Cql SAML.Issuer -} Wire.IdPConfigStore.Orphans ()
import Wire.Migration
import Wire.Postgres
import Wire.SAMLUserStore.Postgres qualified as Postgres
import Wire.Sem.Logger (mapLogger)
import Wire.Sem.Logger.TinyLog (loggerToTinyLog)
import Wire.Util (embedClient)

type EffectStack =
  [ AtomicState Int,
    Input ClientState,
    Input HasqlPoolExt.Pool,
    Resource,
    Async,
    Race,
    TinyLog,
    Embed IO,
    Final IO
  ]

migrateSAMLUsersLoop ::
  MigrationOptions ->
  ClientState ->
  HasqlPoolExt.Pool ->
  Log.Logger ->
  Prometheus.Counter ->
  Prometheus.Counter ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  IO ()
migrateSAMLUsersLoop migOpts cassClient pgPool logger migCounter migFinished migFailed migDuration =
  migrationLoop
    logger
    "saml users"
    migFinished
    migFailed
    (interpreter cassClient pgPool logger "saml users")
    ( migrateAllSAMLUsersV2 migOpts migCounter migDuration
        >> migrateAllLegacySAMLUsers migOpts migCounter migDuration
    )

interpreter :: ClientState -> HasqlPoolExt.Pool -> Log.Logger -> ByteString -> Sem EffectStack a -> IO (Int, a)
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

----------------------------------------------------------------------
-- user_v2 (current table)

selectAllSAMLUsersV2 :: PrepQuery R () (SAML.Issuer, SAML.NameID, UserId)
selectAllSAMLUsersV2 =
  "SELECT issuer, sso_id, uid FROM user_v2"

migrateAllSAMLUsersV2 ::
  ( Member (Input HasqlPoolExt.Pool) r,
    Member (Embed IO) r,
    Member (Input ClientState) r,
    Member TinyLog r,
    Member (AtomicState Int) r,
    Member Resource r,
    Member Race r
  ) =>
  MigrationOptions ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  ConduitM () Void (Sem r) ()
migrateAllSAMLUsersV2 migOpts migCounter migDuration = do
  lift $ info $ Log.msg (Log.val "migrateAllSAMLUsersV2")
  withCount (paginateSem selectAllSAMLUsersV2 (paramsP LocalQuorum () migOpts.pageSize) x5)
    .| logRetrievedPage migOpts.pageSize id
    .| C.mapM_
      ( traverse_ $ \(issuer, subject, uid) ->
          handleErrors (encodeUtf8 (rowId uid)) $
            migrateSAMLUserRow
              migOpts
              migCounter
              migDuration
              probeSAMLUserV2
              Postgres.upsertSAMLUserRow
              (SAML.UserRef issuer subject)
              uid
      )

----------------------------------------------------------------------
-- user (legacy table, swept after user_v2)

selectAllLegacySAMLUsers :: PrepQuery R () (SAML.Issuer, SAML.NameID, UserId)
selectAllLegacySAMLUsers =
  "SELECT issuer, sso_id, uid FROM user"

migrateAllLegacySAMLUsers ::
  ( Member (Input HasqlPoolExt.Pool) r,
    Member (Embed IO) r,
    Member (Input ClientState) r,
    Member TinyLog r,
    Member (AtomicState Int) r,
    Member Resource r,
    Member Race r
  ) =>
  MigrationOptions ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  ConduitM () Void (Sem r) ()
migrateAllLegacySAMLUsers migOpts migCounter migDuration = do
  lift $ info $ Log.msg (Log.val "migrateAllLegacySAMLUsers")
  withCount (paginateSem selectAllLegacySAMLUsers (paramsP LocalQuorum () migOpts.pageSize) x5)
    .| logRetrievedPage migOpts.pageSize id
    .| C.mapM_
      ( traverse_ $ \(issuer, subject, uid) ->
          handleErrors (encodeUtf8 (rowId uid)) $
            -- user_v2 rows were copied first and are authoritative: a stale
            -- legacy row must never overwrite them, so insert-if-absent.
            migrateSAMLUserRow
              migOpts
              migCounter
              migDuration
              probeLegacySAMLUser
              Postgres.insertSAMLUserIfAbsent
              (SAML.UserRef issuer subject)
              uid
      )

-- | Liveness probe for the @user_v2@ table (5622 pattern): re-read the row
-- from Cassandra right before copying it, so a SAML mapping deleted (SSO
-- unlink) after the page was scanned is skipped instead of resurrected in
-- Postgres.
probeSAMLUserV2 :: (Member (Input ClientState) r, Member (Embed IO) r) => SAML.UserRef -> Sem r Bool
probeSAMLUserV2 (SAML.UserRef tenant subject) = do
  client <- input @ClientState
  isJust
    <$> embedClient
      client
      ( retry x1 (query1 probe (params LocalQuorum (tenant, normalizeQualifiedNameId subject)))
      )
  where
    probe :: PrepQuery R (SAML.Issuer, NormalizedUNameID) (Identity UserId)
    probe = "SELECT uid FROM user_v2 WHERE issuer = ? AND normalized_uname_id = ?"

-- | Liveness probe for the legacy @user@ table (raw @sso_id@ key).
probeLegacySAMLUser :: (Member (Input ClientState) r, Member (Embed IO) r) => SAML.UserRef -> Sem r Bool
probeLegacySAMLUser (SAML.UserRef tenant subject) = do
  client <- input @ClientState
  isJust
    <$> embedClient
      client
      (retry x1 (query1 probe (params LocalQuorum (tenant, subject))))
  where
    probe :: PrepQuery R (SAML.Issuer, SAML.NameID) (Identity UserId)
    probe = "SELECT uid FROM user WHERE issuer = ? AND sso_id = ?"

rowId :: UserId -> Text
rowId = UUID.toText . toUUID

----------------------------------------------------------------------
-- per-row copy

migrateSAMLUserRow ::
  ( PGConstraints r,
    Member TinyLog r,
    Member Resource r,
    Member Race r
  ) =>
  MigrationOptions ->
  Prometheus.Counter ->
  Prometheus.Vector Text Prometheus.Histogram ->
  (SAML.UserRef -> Sem r Bool) ->
  (SAML.UserRef -> UserId -> Sem r ()) ->
  SAML.UserRef ->
  UserId ->
  Sem r ()
migrateSAMLUserRow migOpts migCounter migDuration probe writeRow uref uid = do
  -- user id: the issuer/NameID are not secrets, but identify rows uniformly by
  -- user id to keep log fields stable.
  let rowIdent = rowId uid
  outcomeRef <- liftIO $ IORef.newIORef @Text "error"
  bracket
    (liftIO getCurrentTime)
    (observeDuration migDuration outcomeRef)
    ( const $ do
        -- Liveness probe (5622 pattern): the scanned page can be stale, and a
        -- SAML mapping deleted (SSO unlink) after enumeration must not be
        -- resurrected in Postgres by the copy.
        alive <- probe uref
        if not alive
          then markOutcome outcomeRef "skipped-deleted"
          else do
            timeoutResult <-
              Conc.timeout (migOpts.timeout <$ handleTimeout rowIdent) migOpts.timeout $
                writeRow uref uid
            case timeoutResult of
              Left timedOutAfter -> do
                markOutcome outcomeRef "timeout"
                liftIO . UnliftIO.throwIO $ MigrationTimedOut rowIdent timedOutAfter
              Right () -> do
                markOutcome outcomeRef "success"
                liftIO $ Prometheus.incCounter migCounter
    )
  where
    handleTimeout rowIdent =
      err $
        Log.msg (Log.val "saml user migration timed out")
          . Log.field "user_id" rowIdent
          . Log.field "timeout" (show migOpts.timeout)

    markOutcome ref outcome = liftIO $ IORef.writeIORef ref outcome

    observeDuration metric outcomeRef start = do
      outcome <- liftIO $ IORef.readIORef outcomeRef
      end <- liftIO getCurrentTime
      liftIO $ Prometheus.withLabel metric outcome (`Prometheus.observe` realToFrac (diffUTCTime end start))
