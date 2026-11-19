-- activity_visited_areas now stores protected-area ids, admin-region ids and
-- "country:<ISO2>" identifiers. A FK from area_id to protected_areas(id) is no
-- longer valid for those rows. 01-init.sql creates the table without it; this
-- migration removes it from databases initialized by older versions of the
-- schema. Safe to re-run on fresh databases.
ALTER TABLE activity_visited_areas
    DROP CONSTRAINT IF EXISTS activity_visited_areas_area_id_fkey;
