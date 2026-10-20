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

module Wire.ScimExternalIdStore.DualWrite
  ( interpretScimExternalIdStoreToCassandraAndPostgres,
  )
where

import Cassandra (Client)
import Imports
import Polysemy
28: @ours
29: @ours
import Wire.Postgres (PGConstraints)
import Wire.ScimExternalIdStore
import Wire.ScimExternalIdStore.Cassandra qualified as Cassandra
import Wire.ScimExternalIdStore.Postgres qualified as Postgres

-- | Cassandra is the source of truth during migration; writes are mirrored to
-- Postgres.
29: @ours
  LookupStatus tid veid ->
    Cassandra.scimExternalIdStoreToCassandra (Wire.ScimExternalIdStore.lookupStatus tid veid)
