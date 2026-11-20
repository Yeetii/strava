-- PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- 1. Users
CREATE TABLE IF NOT EXISTS users (
    id                TEXT PRIMARY KEY,
    user_name         TEXT,
    first_name        TEXT,
    last_name         TEXT,
    refresh_token     TEXT,
    access_token      TEXT,
    token_expires_at  BIGINT,
    strava_scope      TEXT,
    sync_status       JSONB
);

-- 2. Sessions (ephemeral auth tokens)
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at_utc  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

-- 3. User Sync Items (settings and client app files)
CREATE TABLE IF NOT EXISTS user_sync_items (
    id          TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    category    TEXT NOT NULL,
    key         TEXT NOT NULL,
    updated_at  BIGINT NOT NULL,
    deleted     BOOLEAN NOT NULL DEFAULT FALSE,
    value       JSONB,
    CONSTRAINT uq_user_sync_user_category_key UNIQUE (user_id, category, key)
);
CREATE INDEX IF NOT EXISTS idx_user_sync_lookup ON user_sync_items(user_id, category);

-- 4. Activities (Strava activities)
CREATE TABLE IF NOT EXISTS activities (
    id                   TEXT PRIMARY KEY,
    user_id              TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    description          TEXT,
    sport_type           TEXT NOT NULL,
    start_date           TIMESTAMPTZ NOT NULL,
    start_date_local     TIMESTAMPTZ NOT NULL,
    timezone             TEXT,
    start_lat_lng        REAL[],
    end_lat_lng          REAL[],
    distance             REAL,
    moving_time          REAL,
    elapsed_time         REAL,
    calories             REAL,
    total_elevation_gain REAL,
    elev_high            REAL,
    elev_low             REAL,
    average_speed        REAL,
    max_speed            REAL,
    athlete_count        INTEGER,
    polyline             TEXT,
    summary_polyline     TEXT,
    processing_status    JSONB
);
CREATE INDEX IF NOT EXISTS idx_activities_user_date ON activities(user_id, start_date_local DESC);
CREATE INDEX IF NOT EXISTS idx_activities_user_sport ON activities(user_id, sport_type);

-- 5. Curated Peak Groups
CREATE TABLE IF NOT EXISTS peaks_groups (
    id               UUID PRIMARY KEY,
    parent_id        UUID,
    name             TEXT NOT NULL,
    amount_of_peaks  INTEGER NOT NULL,
    peak_ids         TEXT[] NOT NULL DEFAULT '{}',
    boundary         JSONB
);

-- 6. OSM Domain Table: Peaks
CREATE TABLE IF NOT EXISTS peaks (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    elevation  REAL,
    tile_x     INTEGER NOT NULL,
    tile_y     INTEGER NOT NULL,
    geom       GEOMETRY(Point, 4326) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_peaks_geom ON peaks USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_peaks_tile ON peaks (tile_x, tile_y);

-- 7. OSM Domain Table: Protected Areas
CREATE TABLE IF NOT EXISTS protected_areas (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    area_type          TEXT NOT NULL,
    wikidata           TEXT,
    wikimedia_commons  TEXT,
    tile_x             INTEGER NOT NULL,
    tile_y             INTEGER NOT NULL,
    geom               GEOMETRY(Geometry, 4326) NOT NULL,
    properties         JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_protected_areas_geom ON protected_areas USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_protected_areas_tile ON protected_areas (tile_x, tile_y);

-- 8. OSM Domain Table: Admin Boundaries
CREATE TABLE IF NOT EXISTS admin_boundaries (
    id                 TEXT PRIMARY KEY,
    osm_id             BIGINT NOT NULL,
    osm_type           TEXT NOT NULL,
    admin_level        INTEGER NOT NULL,
    name               TEXT NOT NULL,
    wikidata           TEXT,
    geom               GEOMETRY(Geometry, 4326) NOT NULL,
    geom_simplified    GEOMETRY(Geometry, 4326),
    properties         JSONB NOT NULL DEFAULT '{}',
    metrics_version    INTEGER NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_admin_boundaries_geom ON admin_boundaries USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_admin_boundaries_level ON admin_boundaries (admin_level);

-- 9. OSM Tile Query Cache Tracker (replaces empty-* markers)
CREATE TABLE IF NOT EXISTS osm_tile_queries (
    kind       TEXT NOT NULL,
    zoom       INTEGER NOT NULL,
    x          INTEGER NOT NULL,
    y          INTEGER NOT NULL,
    queried_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, zoom, x, y)
);

-- 10. Junction Table: Activity Summits
CREATE TABLE IF NOT EXISTS activity_summits (
    activity_id  TEXT NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    peak_id      TEXT NOT NULL REFERENCES peaks(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_id, peak_id)
);
CREATE INDEX IF NOT EXISTS idx_activity_summits_user ON activity_summits(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_summits_peak ON activity_summits(peak_id);

-- 11. Junction Table: Activity Visited Paths
-- One row per activity and z12 tile. highway_ids stores the numeric part of
-- the OSM id (e.g. 123 for "way/123"). All queries are user-scoped.
CREATE TABLE IF NOT EXISTS activity_visited_tiles (
    activity_id  TEXT NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tile_x       INTEGER NOT NULL,
    tile_y       INTEGER NOT NULL,
    highway_ids  BIGINT[] NOT NULL,
    PRIMARY KEY (activity_id, tile_x, tile_y)
);
CREATE INDEX IF NOT EXISTS idx_activity_visited_tiles_user_tile
    ON activity_visited_tiles (user_id, tile_x, tile_y);

-- 12. Junction Table: Activity Visited Areas
CREATE TABLE IF NOT EXISTS activity_visited_areas (
    activity_id  TEXT NOT NULL REFERENCES activities(id) ON DELETE CASCADE,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    area_id      TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_id, area_id)
);
CREATE INDEX IF NOT EXISTS idx_activity_visited_areas_user ON activity_visited_areas(user_id);
CREATE INDEX IF NOT EXISTS idx_activity_visited_areas_area ON activity_visited_areas(area_id);
