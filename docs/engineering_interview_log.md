# SIGNAL Engineering & Interview Log

This is a running record of engineering issues encountered while implementing SIGNAL. It distinguishes verified failures from design questions and safeguards. Entries are updated as the project progresses.

## Verified issues and fixes

### GitHub rate-limit header casing

- **Symptom:** Two GitHub API client tests failed: the simulated rate-limit 403 case did not raise the expected exception, and the quota-floor check did not recognize the remaining-quota header.
- **Root cause:** The client looked up rate-limit headers using lowercase dictionary keys. The test adapter supplied uppercase header names. Real HTTPX headers are case-insensitive, but ordinary test dictionaries are not.
- **Fix:** Normalize all response header names to lowercase before reading `x-ratelimit-remaining`.
- **Verification:** The GitHub client and connector tests passed after the fix; the full local suite subsequently reached 71 passing tests. After adding Remotive, the user confirmed 90 tests passed locally.
- **Engineering lesson:** HTTP header names are case-insensitive by specification. Adapters and test doubles should preserve that semantic rather than rely on one representation's key lookup behaviour.

### SQLAlchemy mapped-table wrapper in the runner's fake database adapter

- **Symptom:** Five focused runner tests failed and one errored. Successful scope processing was reported as failed, so duplicate handling, partial-scope behaviour, request spacing and skipped-ID reporting also appeared broken. The simulated commit-failure test raised while finalizing the run instead of during scope persistence.
- **Root cause:** The test fake identified an UPDATE with object identity: `statement.table is Source.__table__`. SQLAlchemy exposes the mapped UPDATE target as an `AnnotatedTable`, so the identity check was false. The fake raised an assertion during the scope transaction; because that transaction never reached its simulated commit, the configured commit counter reached its failure point later during run finalization.
- **Fix:** Match the UPDATE target by its stable table name: `statement.table.name == Source.__tablename__`.
- **Verification:** Fix committed as `5320e4b`; awaiting the user's focused local rerun. Do not count this as resolved until the tests pass.
- **Engineering lesson:** ORM-generated SQL expressions may wrap mapped tables. Test adapters should match stable SQL metadata (such as table name) instead of relying on Python object identity.

### SQLAlchemy UPDATE parameter names in the fake cursor persistence test

- **Symptom:** After fixing the fake adapter's mapped-table identity check, scope persistence still failed. The fake test code attempted to read hard-coded compiled parameter names and misidentified the cursor value.
- **Root cause:** SQLAlchemy generates bind parameter names based on the expression tree; those names are not a stable contract for the test adapter.
- **Fix:** The fake now identifies bound values by type: the scope key is a non-UUID string, and the cursor is the dictionary value. This avoids depending on generated parameter names.
- **Verification:** Committed as `45cd714`; awaiting local focused tests.
- **Engineering lesson:** Avoid coupling tests to incidental SQLAlchemy bind names. Assert on semantic values instead.

## Open design / observability questions

### Remotive skipped-job count is not exposed to the runner

- **Observed behaviour:** The Remotive connector skips jobs without a usable ID and logs the count before returning its `FetchResult`.
- **Contract limitation:** The approved `FetchResult` contains only `events` and `next_cursor`; it has no skipped-record count or warning metadata.
- **Why it matters:** The Phase 2 decisions require skipped native-ID events to be counted and mentioned in `ingestion_runs.error_message`. The runner cannot report a count that the connector has already discarded.
- **Current handling:** Keep the approved connector contract unchanged. The runner counts and reports invalid `RawEvent` objects it actually receives. The upstream Remotive skipped count remains an explicit open question for Claude/owner review; no extra cursor field, quarantine table, or contract extension is being introduced without approval.
- **Interview lesson:** A component boundary can hide operational metadata even when the underlying component logs it. The right fix requires an explicit contract decision, not guessing where to smuggle metrics.

## Verified local checkpoints

| Checkpoint | Result |
|---|---:|
| GitHub client/connector and earlier Phase 2 suite | 71 tests passed |
| Remotive connector and full Phase 2 suite | 90 tests passed |

*New issues, diagnostics, fixes, and local verification results will be appended as they occur.*
