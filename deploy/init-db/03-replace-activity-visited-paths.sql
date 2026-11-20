-- Replaces the row-per-visit activity_visited_paths table with one row per
-- activity and z12 tile. The existing data is intentionally dropped: the
-- visited paths worker rebuilds the table from activity polylines.
--
-- Safe to re-run on fresh databases (01-init.sql already creates the new table).

DROP TABLE IF EXISTS activity_visited_paths;

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
