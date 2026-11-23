# SIGNAL Engineering & Interview Log

This is a running record of engineering issues encountered while implementing SIGNAL. It distinguishes verified failures from design questions and safeguards. Entries are updated as the project progresses.

## Verified issues and fixes

### GitHub rate-limit header casing

- **Symptom:** Two GitHub API client tests failed: the simulated rate-limit 403 case did not raise the expected exception, and the quota-floor check did not recognize the remaining-quota header.
- **Root cause:** The client looked up rate-limit headers using lowercase dictionary keys. The test adapter supplied uppercase header names. Real HTTPX headers are case-insensitive, but ordinary test dictionaries are not.
- **Fix:** Normalize all response header names to lowercase before reading `x-ratelimit-remaining`.
- **Verification:** The GitHub client and connector tests passed after the fix; the full local suite subsequently reached 71 passing tests. After adding Remotive, the user confirmed 90 tests passed locally.
- **Engineering lesson:** HTTP header names are case-insensitive by specification. Adapters and test doubles should preserve that semantic rather than rely on one representation's key lookup behaviour.

### SQLAlchemy expression and INSERT representations in the runner's fake database adapter

- **Observed symptom:** The runner's first focused test run failed across several persistence scenarios. A diagnostic run later exposed the concrete scope error: `KeyError: 'source_id'` inside the fake adapter's event INSERT handling. The other failures were downstream because the first scope never completed.
- **First mismatch found:** The fake adapter compared `statement.table` to `Source.__table__` by object identity. SQLAlchemy wraps mapped UPDATE targets in an `AnnotatedTable`, so identity comparison was unreliable.
- **Correction:** Match the UPDATE target using `statement.table.name == Source.__tablename__`. This was committed as `5320e4b`.
- **Second mismatch found:** The fake UPDATE handler relied on SQLAlchemy-generated bind parameter names. Those names are implementation details, not a stable test contract.
- **Correction:** Extract the scope key and cursor from the compiled parameter values by their semantic types, rather than hard-coded generated names. This was committed as `45cd714`.
- **Confirmed INSERT root cause:** A diagnostic inspection of `statement._multi_values` showed that each row is a dictionary keyed by SQLAlchemy `Column` objects, not strings. The fake adapter attempted `values["source_id"]`, which raised the observed `KeyError`.
- **Correction:** Normalize each INSERT row to string keys using `column.key` before applying the fake in-memory uniqueness and row-count behaviour. Committed as `51bc102`.
- **Verification:** The focused local runner suite has **not yet passed** after these test-adapter corrections. Keep this issue open until the user reruns all 10 focused tests successfully; only then run the full suite.
- **Engineering lesson:** SQLAlchemy expression objects and compiled statements have structured representations. A test double should inspect stable semantic metadata, not assume plain-string mapping keys, object identity, or generated bind-parameter names.

### Fake-session query recorder omitted scalar SELECT statements

- **Symptom:** After the persistence-path tests passed, the remaining focused test failed because it expected two `SELECT ... FOR UPDATE` statements but found zero in the fake store's recorded statement list.
- **Root cause:** The fake session appended statements in `execute()`, but the runner issues the source-row locking query through `session.scalar()`. That method executed the fake lookup without recording the statement, so the assertion inspected an incomplete query history.
- **Fix:** Record the statement at the start of the fake `scalar()` method. The production runner's `with_for_update()` query is unchanged.
- **Verification:** The user confirmed all 10 focused runner tests pass locally after pulling commit `e790760`. The full test suite is the next verification step; the runner is not considered fully verified until that suite passes.
- **Engineering lesson:** When a test double records database activity, every supported execution path (`execute`, `scalar`, `scalars`, etc.) must be represented consistently; otherwise tests can report missing behaviour that actually occurred.

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
