# Copilot Instructions

## Cosmos DB Performance

Documents in our Cosmos containers can be very large (multi-thousand lines). Always prefer:
- **Patch operations** (`PatchDocument` / `PatchOperation.Set` / `PatchOperation.Remove`) over full document upserts when updating a few fields.
- **Projected queries** (`SELECT c.id, c.x, c.y, c.properties.foo`) over `SELECT *` when only a subset of fields is needed.

## Bruno API Collection

All API endpoints must have a corresponding Bruno request file in the `bruno/` folder.
When adding, renaming, or removing an endpoint in the API project, update the matching `.bru` file under the appropriate subfolder (e.g. `bruno/^manage/` for admin endpoints).
