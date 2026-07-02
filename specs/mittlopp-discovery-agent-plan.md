# Mittlopp Discovery Agent Plan

## Summary

Build a dedicated `mittlopp.se` discovery source in the existing C# discovery pipeline.

Use the aggregate calendar at `/kalender` as the canonical discovery surface. Do not rely on category pages for completeness or type accuracy.

For internal Mittlopp events, enrich calendar cards by fetching the linked `/anm/...` page and, when useful, one linked registration variant page to extract richer structured metadata.

## Live Site Findings

Validated against the live site using read-only fetches during spec refinement.

- `/kalender` is the most reliable primary discovery surface.
- Pagination works with normal links like `/kalender/?page=2`, `/kalender/?page=3`.
- Aggregate calendar mixes internal Mittlopp event pages and external event websites.
- Category pages are not reliable enough to drive discovery policy.
- `/kalender/cykel` includes clearly non-cycling events.
- Category pages may omit or misclassify events relative to the aggregate calendar.
- Internal Mittlopp event URLs come in two useful shapes: event hub pages like `/anm/satertriathlon2026`, and registration variant pages like `/anm/satertriathlon2026/PEYPFWREAWHIYOGCRA?lang=sv`.
- Event hub pages usually expose event name, location, organizer, organizer website or homepage, shared event description, and links to one or more registration variants.
- Registration variant pages usually expose the richest structured race metadata: sport or local subtype, distance or discipline breakdown, exact date or start time, price or price tiers, and registration deadline.
- Real malformed date examples exist on the aggregate calendar: `2026-07-01–0001-01-01`, `2026-07-04–05`, `2026-08-15–10-15`.

## Canonical Discovery Policy

- Discover from `/kalender` only.
- Treat category pages as diagnostics only.
- Include calendar events only if they map to a supported canonical `RaceType`.
- Infer canonical type from content, not from the category page path.
- Preserve local semantics in `TypeLocal` for retained events only.
- Drop unsupported activities during discovery rather than storing partial records.

## Supported Canonical Types

Mittlopp discovery only emits events that map to one of these canonical types:

- `running`
- `cycling`
- `triathlon`
- `swimrun`
- `obstacle-course`

If a discovered event does not map confidently to one of the supported canonical types, drop it from discovery output.

Examples:

- keep `Trail`, `Marathon`, `Backyard`, and standard running races as `running`
- keep `Cykel`, `Gravel`, and `MTB` as `cycling`
- keep `Sprint`, `Olympisk`, `Supersprint`, `Medeldistans`, `Långdistans`, `Terrängtriathlon`, and similar triathlon formats as `triathlon`
- keep `Swimrun` as `swimrun`
- keep `Hinderbana` as `obstacle-course`
- drop `Duathlon`
- drop `Promenad`
- drop `Simskola`
- drop unsupported activities unless an explicit supported mapping is added later

## Discovery Strategy

1. Fetch `/kalender`.
2. Parse event cards from the page.
3. Follow next-page links until none remain or a normalized next URL repeats.
4. Deduplicate events by resolved event URL.
5. For external event links:
   - keep calendar metadata only
   - store the external URL as `WebsiteUrl`
   - infer canonical type from card content
   - drop the event if no supported canonical type can be inferred
6. For internal `/anm/...` links:
   - fetch the event hub page
   - extract shared metadata and any registration variant links
   - optionally fetch one registration variant page for richer structured data
   - infer canonical type from combined calendar and detail-page content
   - drop the event if no supported canonical type can be inferred

## Why Not Category Pages

- Category pages are not a stable canonical source.
- `/kalender/cykel` contains unrelated events.
- Aggregate calendar already exposes mixed event types and paginates correctly.
- Canonical mapping is safer when inferred from event content than when trusted from category taxonomy.

## Page Shapes

### Aggregate calendar page

Calendar cards can provide:

- event URL
- name
- image URL
- date text
- location text
- summary or distance text
- badge icons or badge titles

Card links may target either:

- internal Mittlopp `/anm/...` pages
- external event websites

### Internal event hub page

Top-level `/anm/...` pages often act as an event hub rather than a fully specific race-detail page.

Extract where available:

- event name
- location
- homepage or external website
- organizer name
- organizer page URL
- shared event description
- registration variant links
- event logo if useful

### Internal registration variant page

Registration variant pages are the preferred source for structured race metadata when available.

Extract where available:

- local sport label
- local subtype label
- exact date
- exact start time
- place
- homepage or external website
- organizer name
- distance summary
- discipline subparts
- price or price tiers
- registration deadline
- long description

For triathlon-like pages, construct a clearer distance string from subparts when present, such as:

- `1500 m swim, 40 km bike, 10 km run`

## Registration Variant Selection

When an internal event hub links to multiple registration variants, choose one canonical variant page for enrichment.

Recommended selection order:

1. first variant that exposes structured metadata such as sport, distance, start time, and price
2. prefer a standard main race variant over children-only or novelty variants when multiple candidates exist
3. otherwise use the first listed variant link

Do not fetch every variant page unless implementation complexity stays low and there is a concrete need.

## Fields To Populate

Populate these `ScrapeJob` fields where available:

- `Name`
- `Date`
- `Location`
- `Distance`
- `RaceType`
- `TypeLocal`
- `ImageUrl`
- `Organizer`
- `Description`
- `StartFee`
- `Currency`
- `WebsiteUrl`
- `ExternalIds`

Recommended rules:

- `WebsiteUrl`: for external calendar cards, preserve the external destination; for internal events, prefer an external homepage when the page exposes one; otherwise keep the Mittlopp URL
- `Currency = "SEK"` when a `kr` price is extracted
- `TypeLocal` should preserve retained local labels like `Motion`, `Sprint`, `Olympisk`, `Terrängtriathlon`, `Swimrun`, `Hinderbana`
- `StartFee` should use the lowest visible fee or the first visible fee tier when several are shown
- `ExternalIds` should include a stable Mittlopp identifier when one can be derived from the canonical internal URL

## Type Inference

Infer canonical type from content instead of trusting category pages.

### Triathlon

Use `triathlon` when name, summary, badges, or detail-page content suggests:

- `triathlon`
- `sprint`
- `supersprint`
- `olympisk`
- `medeldistans`
- `långdistans`
- `terrängtriathlon`
- triathlon-specific badges or icons

Do not map `duathlon` to `triathlon`.

### Cycling

Use `cycling` when content suggests:

- `cykel`
- `gravel`
- `mtb`
- `bike`

### Running

Use `running` when content suggests:

- `löpning`
- `trail`
- `marathon`
- `backyard`
- `lopp`
- standard km-based races without stronger cycling, triathlon, swimrun, or obstacle-course signals

### Swimrun

Use `swimrun` when content suggests:

- `swimrun`

### Obstacle course

Use `obstacle-course` when content suggests:

- `hinderbana`

### Unsupported labels

Drop the event when content only suggests unsupported local activity types such as:

- `duathlon`
- `promenad`
- `simskola`

## Detail Page Extraction Rules

For internal `mittlopp.se/anm/...` events:

1. start with the calendar card seed job
2. fetch the event hub page
3. merge shared metadata from the hub page
4. if a useful registration variant link exists, fetch one variant page and merge richer structured fields
5. infer canonical type from the combined evidence
6. discard the job if the resulting canonical type is unsupported or null

If multiple price tiers are shown:

- use the lowest visible fee or the first visible fee tier as `StartFee`
- set `Currency = "SEK"`
- optionally append fee-tier context into `Description` if it is useful and easy to preserve cleanly

## Parsing Targets

### Calendar page

Extract:

- href
- name
- image URL
- date text
- location text
- summary or distance text
- badge titles, badge labels, or icon hints

### Next-page handling

Use the next-page link from the aggregate calendar only.

Stop when:

- no next-page link is found
- next URL repeats after normalization

## Normalization Rules

- Normalize dates to `yyyy-MM-dd` where possible.
- Prefer exact variant-page date over hub-page date over calendar date.
- For malformed or range-like calendar dates, normalize to the start date only.
- Normalize these examples to the first date component: `2026-07-01–0001-01-01` -> `2026-07-01`, `2026-07-04–05` -> `2026-07-04`, `2026-08-15–10-15` -> `2026-08-15`
- Preserve unresolved date text only if normalization fails completely.
- Normalize relative URLs against the page URL.
- Deduplicate by resolved absolute event URL.

## Expected Edge Cases

- malformed date ranges on calendar cards
- events with no internal detail page beyond the calendar card
- external-only event URLs
- internal event hubs with many registration variants
- multi-distance internal events
- badge-driven type hints with weak text labels
- unsupported local activities that should be filtered out

## Implementation Shape

### New discovery worker class

Add a dedicated discovery worker class:

- `Backend/DiscoverMittloppRaces.cs`

Responsibilities:

- enqueue the Mittlopp discovery agent on schedule
- process one aggregate calendar page at a time
- write retained discoveries with source `mittlopp`
- enqueue the next page when appropriate

### New parser and enrichment agent

Add a dedicated Mittlopp discovery helper:

- `Backend/MittloppDiscoveryAgent.cs`

Suggested methods:

- `ParseCalendarPage(string html, Uri pageUrl)`
- `ExtractNextPageUrl(string html, Uri pageUrl)`
- `EnrichFromEventHubPage(ScrapeJob job, string html, Uri pageUrl)`
- `ExtractRegistrationVariantUrls(string html, Uri pageUrl)`
- `EnrichFromRegistrationVariantPage(ScrapeJob job, string html, Uri pageUrl)`
- `InferRaceType(...)`
- `ShouldKeepJob(...)`

This matches the existing dedicated-agent pattern already used by sources like DUV and Skyrunning.

### Worker registration

Register the new class in:

- `Backend/Program.cs`

Wire a new agent key in:

- `Backend/RaceDiscoveryWorker.cs`

Recommended key:

- `"mittlopp"`

## Concurrency And Failure Policy

- paginate calendar sequentially
- enrich internal detail pages with bounded concurrency, likely `SemaphoreSlim(4)`
- if enrichment fails for an internal event, keep the calendar seed job only if it still maps to a supported canonical type
- if type inference remains unsupported or null after fallback, drop the job

## Testing Plan

Add unit tests for:

1. aggregate calendar parsing with mixed internal and external links
2. next-page extraction from `/kalender/?page=N`
3. event hub extraction
4. registration variant extraction of start time, fee, deadline, and homepage
5. triathlon distance synthesis from swim, bike, and run subparts
6. type inference for running, cycling, triathlon, swimrun, and obstacle-course
7. filtering of unsupported types such as `duathlon`, `promenad`, and `simskola`
8. malformed date normalization
9. duplicate event URL normalization

Recommended fixtures:

- one aggregate calendar page
- one internal triathlon event hub page
- one internal triathlon registration variant page
- one external-link event card
- one swimrun event
- one hinderbana event
- one unsupported event that should be dropped

Recommended test location:

- `Backend.Tests/RaceScrapeDiscoveryTests.cs`

## Final Policy

- discover from `/kalender` only
- treat category pages as diagnostics only
- infer canonical type conservatively from content
- retain only events that map to `running`, `cycling`, `triathlon`, `swimrun`, or `obstacle-course`
- preserve `TypeLocal` only for retained events
- drop unsupported types including `duathlon`
