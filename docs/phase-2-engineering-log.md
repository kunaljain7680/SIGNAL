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
- GitHub file writes: not yet performed at the time this entry was drafted.
- Automated tests: not run in the user's local environment.
- Migration: not applied to any database.
- External provider integration: not yet tested.
- Local manual verification: pending user-provided Git and Alembic state.

No Phase 2 acceptance criterion is marked complete by this schema-only unit.
