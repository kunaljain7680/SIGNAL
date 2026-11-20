# SIGNAL — Phase 2 Engineering Log

**Phase:** 2 — Real Ingestion  
**Architecture:** `signal-architecture.md` (locked; §C and §AB)  
**Working branch:** `implementation/full-architecture`  
**Started:** 2026-11-21

This log records implementation decisions, deviations from the locked architecture, problems encountered, and actual verification evidence. Planned behavior is not recorded as completed behavior. Test and migration results are added only after they are observed.

## Approved implementation decisions

| Area | Decision |
|---|---|
| Sources | Keep GitHub, Hacker News (Algolia), and Remotive Jobs |
| Connector contract | Preserve `SourceConnector.fetch_since(cursor) -> list[RawEvent]`; source-specific cursor objects carry the next state |
| Cursor persistence | Add `sources.cursor_state JSONB`; do not create a separate cursor table |
| Persistence boundary | Insert raw events and advance cursor state in one database transaction |
| Ingestion runs | Reuse `ingestion_runs`; add `FAILED` for a terminal Phase 2 failure state |
| Reliability scope | Record failure and isolate sources in Phase 2; retries, backoff, DLQ and recovery remain Phase 7 |
| Scheduler | GitHub Actions scheduled workflow for cloud execution, plus a local manual command |
| HN | Reuse the existing Algolia client and Phase 1 HN pipeline behavior; preserve `hn_v0` scoring |

## Architecture deviation: Remotive incremental fetching

**Architecture reference:** §C.2 specifies `since_id` / page cursor.

**Verified provider behavior:** Remotive's public API documentation describes a feed of all active remote jobs, sorted by publication date, with a `limit` parameter and category/company/search filters. It does not document a `since_id` parameter or page-based pagination. The provider recommends no more than four API requests per day, discloses a 24-hour listing delay, and requires attribution to Remotive plus a link to the original listing.

**Approved adaptation:** Retain Remotive. Fetch the bounded active-jobs feed at most once per day, filter matching tracked technologies locally, maintain a local publication-date/job-ID watermark in `sources.cursor_state`, and rely on the existing unique `(source_id, source_native_id)` constraint for durable deduplication.

**Trade-off:** Watermarking prevents reprocessing older feed records downstream, but the API still returns the active feed on each daily poll. It is not equivalent to a server-side incremental cursor. Revisit only if real response size, truncation, or ingestion tests demonstrate that the approach is inadequate.

**Provider references:**
- https://remotive.com/remote-jobs/api
- https://github.com/remotive-com/remote-jobs-api

## Scheduling constraint

GitHub documents that `schedule` workflows run from the repository's default branch and that scheduled runs may be delayed or dropped during periods of high load. The default branch is currently `main`, which must remain untouched without explicit approval.

Therefore, a scheduled workflow committed only to `implementation/full-architecture` is prepared but is not yet an active cloud schedule. Activating the schedule requires the workflow file to exist on the default branch, which requires the user's later explicit approval to merge. The implementation branch remains independently testable with the local manual command.

Reference: https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule

## Phase 2 boundary

Phase 2 records source-specific run start, completion, or failure and ensures one source's failure does not suppress attempts for other sources. It does not implement retries, exponential backoff, dead-letter handling, recovery endpoints, checkpointed crash-resume, async fan-out, or bounded worker pools. Those remain assigned to later architecture phases.

## Implementation and verification record

### 2026-11-21 — persistence schema groundwork

**Implemented in this unit:**
- Added a separate non-null `sources.cursor_state JSONB` column with an empty-object default.
- Added `FAILED` to the `ingestion_runs` status check constraint.
- Added model-level tests for the new cursor-state column and status model check.
- Added this engineering log.

**Migration:** `f0a2b43c1d7e` follows `c7d91a4e2f60`.

**Verification status:**
- GitHub commit: `a55b2034130c336a1e1515129babd2652f61dade`; changed files were re-read from the remote branch after commit.
- Automated tests: not run in the user's local environment.
- Migration: not applied to any database.
- External provider integration: not yet tested.
- Local manual verification: pending user-provided Git and Alembic state.

No Phase 2 acceptance criterion is marked complete by this schema-only unit.


### 2026-11-21 — common connector contract and HN adapter

**Implemented in this unit:**
- Added shared `Cursor`, `RawEvent`, `SourceHealth`, `RateLimitConfig`, and `AuthConfig` types plus the original `SourceConnector` protocol.
- Added an HN connector that uses per-keyword timestamp watermarks and date-sorted pagination.
- Extended the existing Algolia client with a separate date-sorted paged method; the Phase 1 relevance-sorted method is unchanged.
- Deduplicated overlapping hits across tracked keywords in the connector result.
- Kept cursor advancement in memory until every keyword/page fetch succeeds.
- Aligned the SQLAlchemy `IngestionRun` model check constraint with the migration's new `FAILED` status.

**Verification status:**
- Connector and HN pagination tests were added; they have not been run locally.
- The existing Phase 1 pipeline is not changed by this adapter.
- Database integration and event/cursor atomicity are not yet implemented or verified.
- No migration has been applied.


### 2026-11-21 — GitHub connector

**Implemented in this unit:**
- Added an authenticated GitHub REST client using a token supplied at runtime; no token is stored in source configuration or committed files.
- Added per-topic repository search, repository snapshots and release retrieval.
- Added repository and release ETag persistence in the in-memory cursor; HTTP 304 responses create no raw events.
- Added per-topic `last_checked_at` state.
- Added rate-limit header tracking and a stop-before-next-request guard when the advertised remaining quota is below 10%.
- Limited each tracked topic to the ten most recently updated repository search results per run; this bounds API usage for the seeded catalogue.

**Verification status:**
- GitHub client and connector tests were added; they have not been run locally.
- External GitHub API integration has not yet been tested.
- The connector is not yet wired into the database-backed ingestion-run executor.
- No Phase 2 acceptance criterion is marked complete by this unit alone.


### 2026-11-21 — Remotive Jobs connector

**Implemented in this unit:**
- Added the public Remotive API client at the documented `/api/remote-jobs` endpoint.
- Added local filtering against tracked technology names while preserving each raw job object as the stored payload.
- Added a seven-day initial lookback and a publication-date watermark with a one-day overlap.
- Added a 20-hour local fetch guard so manual reruns do not repeatedly call the provider; the scheduled workflow is intended to run once per day.
- Added the special-case Go language matcher to reduce false positives from the ordinary English word "go".
- Kept the provider response contract honest: a full active-jobs feed is fetched; no undocumented since_id/page parameter is used.

**Verification status:**
- Remotive parsing, watermark, request guard and keyword-filter tests were added; they have not been run locally.
- No live Remotive API call has yet been verified from the application.
- The connector is not yet wired into the database-backed ingestion-run executor.


### 2026-11-21 — shared ingestion executor and scheduled entry point

**Implemented in this unit:**
- Added a database-backed runner that records `PENDING → FETCHING → COMPLETED/FAILED` lifecycle state.
- Added PostgreSQL idempotent raw-event inserts using the existing `(source_id, source_native_id)` unique constraint.
- Added a compare-and-swap update against the previous `sources.cursor_state`; raw inserts, cursor advancement, source health, and run completion commit in one transaction.
- On fetch/persistence failure, the transaction is rolled back and the run is marked `FAILED` in a separate transaction. A later source is still attempted.
- Added source records for GitHub and Remotive through a follow-up seed migration.
- Added a manual CLI and a GitHub Actions schedule workflow.
- Added database integration tests for double-fetch idempotency and per-source failure isolation.

**Scheduling limitation:** the GitHub Actions workflow is committed to the implementation branch. It will not run on a cron schedule until it is present on the repository default branch. `main` remains untouched; no merge has been made.

**Verification status:**
- Code was committed remotely; tests have not been executed locally.
- The database integration tests require PostgreSQL and the Phase 2 migrations to be applied.
- No migration has been applied by this implementation.
- The GitHub Actions workflow has not been run; the required repository secrets and default-branch activation are not yet configured.
