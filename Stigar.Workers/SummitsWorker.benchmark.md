# SummitsWorker Benchmark Findings

This note documents the benchmark that compared the old tile-based worker candidate fetch with the new worker geospatial fetch using the tiled client.

## Goal

Evaluate whether `SummitsWorker` should replace its current `FetchByTiles(...)` candidate lookup with the new geospatial worker path built from `FindFeaturesWithinRadius(...)`.

## Benchmark Setup

- Benchmark utility: `Tools/SummitsFetchBenchmark`
- Data source: live Cosmos DB data from local settings
- Candidate strategies compared:
  - `old`: distinct route tiles, then `FetchByTiles(...)`
  - `new`: the current worker geospatial path, calling `FindFeaturesWithinRadius(...)` per distinct route point and deduplicating locally
- RU is now measured through `TiledCollectionClient` rather than via the standalone benchmark query path
- Summit detection after candidate fetch was kept the same in both cases
- Activity sample selection was biased toward higher route tile fan-out to give the new path a fairer chance

## Results

### Final 5-activity worker comparison

- Old tile average: `96.2 ms`, `4.0 RU`
- New geo average: `2348.5 ms`, `2573.7 RU`
- Runtime delta: `+2341.1%`
- RU delta: `+63542.9%`
- Summit mismatches: `0 / 5`

## Conclusion

The new worker geospatial strategy is correct but dramatically more expensive than the old tile fetch. It preserved summit results in every sampled activity, but it was much slower and consumed orders of magnitude more RU.

Because of that, `SummitsWorker` keeps the tile-based candidate fetch as the default strategy.

## Current Code Decision

- Default strategy: `tile`
- Optional strategy flag: `SummitsWorkerPeakFetchStrategy=geospatial`
- Reason: the measured worker geospatial path is currently far too expensive to justify as the default