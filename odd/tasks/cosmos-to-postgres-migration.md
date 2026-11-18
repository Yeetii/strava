# Cosmos DB to PostgreSQL + PostGIS Migration
Branch: feature/cosmos-to-postgres-migration
Delivery strategy: ask-on-risk
Test runner: dotnet test

## Specs

S1: "Lets plan the migration away from cosmos db, help me decide which replacement database to go with on the self hosted server. Map all the document types stored to determine if a document database makes sense or not. Some things like osmFeatures might make more sense in redis since it is essentially a cache anyways."
S2: "I'll probably skip moving over the calculated metrics and the osm cache and let the new server recalculate those as a stress test and to make migration easier. So we would just need to move over users, activities, userSyncItems. And there is no need to keep the site operational at all time so we might just move everything at once."
S3: "wouldn't just summited_peak_activities be enough though? peak name and elevation would be a join with peaks instead"
S4: "ok go with 3 domain tables. summarize the task"
S5: "get started on the job, use subagents"

## Tasks

T1 (S1, S2): Add PostgreSQL + PostGIS service to deploy/docker-compose.yml and create database initialization DDL with core and domain tables | route: inline | commit: pending
T2 (S1, S2): Build Cosmos-to-PostgreSQL export and bulk import CLI tool for users, sessions, userSyncItems, activities, peaksGroups | route: delegated | commit: pending
T3 (S1, S2): Implement PostgreSQL Core Data Access Layer for users, sessions, userSyncItems, activities | route: delegated | commit: pending
T4 (S3, S4): Implement PostGIS Spatial Repositories for peaks, protected_areas, admin_boundaries, osm_tile_queries | route: delegated | commit: pending
T5 (S3, S4): Update SummitsWorker, VisitedPathsWorker, VisitedAreasWorker and API endpoints to use relational tables and junction tables | route: inline | commit: pending

## Log

L1: 2026-11-18 - User: "Lets plan the migration away from cosmos db, help me decide which replacement database to go with on the self hosted server. Map all the document types stored to determine if a document database makes sense or not. Some things like osmFeatures might make more sense in redis since it is essentially a cache anyways."
L2: 2026-11-18 - Analyzed Cosmos containers, domain relational shape, and Redis limitations for geometry/ST_Intersects. Recommended PostgreSQL + PostGIS.
L3: 2026-11-18 - User agreed to move to relational database, decided to skip moving calculated metrics and OSM cache to let server recalculate them as a stress test, and identified that summits/visits should be junction tables with peaks/areas/paths domain tables.
L4: 2026-11-18 - User chose 3 domain tables for OSM features (peaks, protected_areas, admin_boundaries).
L5: 2026-11-18 - User authorized implementation: "get started on the job, use subagents". Created odd/tasks/cosmos-to-postgres-migration.md and Engram mirror.
