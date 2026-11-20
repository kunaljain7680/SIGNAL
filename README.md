# SIGNAL

**Focus on what matters. Leave the noise.**

SIGNAL is being built according to the locked architecture in `signal-architecture.md`.

## Phase 1 — First Real Signal

- The curated technology catalogue is read from PostgreSQL.
- Hacker News is the only implemented evidence source in this phase.
- HN ingestion is triggered manually from the backend script.
- The `hn_v0` score maps normalized HN events in a seven-day window to a deterministic score stored in `signal_snapshots`.
- Domain relevance uses only `technology.domain_tags`, manually entered `target_domains`, and `interests`.
- The HTML dashboard reads saved backend state; opening or refreshing the page does not run ingestion or calculate a signal.

This phase does not implement multi-source adoption scoring, recommendation categories, skill adjacency, learning-effort weighting, persistent career profiles, scheduled ingestion, or AI enrichment. Those remain in their later architecture phases.

The Phase 1 profile fields (`target_domains` and `interests`) are held in browser local storage and submitted to the domain-relevance API for calculation. They are not persisted in PostgreSQL. Persistent career profiles belong to Phase 5.

## Run locally (Windows PowerShell)

Start PostgreSQL from the repository root:

```powershell
cd F:\SIGNAL
docker compose up -d db
```

Start the API in a second terminal:

```powershell
cd F:\SIGNAL\backend
.\venv\Scripts\Activate.ps1
alembic upgrade head
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Save the supplied Phase 1 dashboard HTML as `frontend/index.html` in the repository. Then serve the dashboard in a third terminal:

```powershell
cd F:\SIGNAL\frontend
python -m http.server 5500 --bind 127.0.0.1
```

Open `http://127.0.0.1:5500` in the browser.

## Run HN ingestion manually

From the backend directory, with the virtual environment active:

```powershell
python scripts/run_hn_pipeline.py --technology "FastAPI"
```

Replace `FastAPI` with an exact `canonical_name` from the seeded catalogue to run another technology. The script stores raw HN source events, normalizes them, counts the seven-day window, and persists the `hn_v0` snapshot. Refresh the dashboard afterward to read the newly saved state.

## Phase 1 API surface used by the dashboard

- `GET /health` — confirms database connectivity.
- `GET /technologies/` — reads the technology catalogue.
- `GET /technologies/{technology_id}/signal` — reads the latest persisted snapshot; it does not calculate one.
- `GET /technologies/{technology_id}/evidence` — reads normalized evidence and its original source identity.
- `POST /technologies/domain-relevance` — calculates domain-only relevance from supplied profile inputs; it does not save the profile or change signal scores.

The API enables CORS only for the two local dashboard origins used above (`127.0.0.1:5500` and `localhost:5500`).


## Phase 2 — Run real source ingestion manually

The shared Phase 2 runner fetches raw evidence independently from the active Hacker News, GitHub, and Remotive sources. It records each attempt in `ingestion_runs`, writes raw source events idempotently, and advances the source cursor in the same database transaction.

From the backend directory, with the repository virtual environment active:

```powershell
cd F:\SIGNAL\backend
.\venv\Scripts\Activate.ps1
python scripts/run_ingestion.py --source all
```

Run one source in isolation with `--source hn`, `--source github`, or `--source jobs`. GitHub requires a read-only token supplied through the `GITHUB_TOKEN` environment variable. Do not put API tokens in `sources.config` or commit them.

Remotive's public API returns its active remote-jobs feed and does not document a server-side incremental cursor. SIGNAL therefore fetches it at most once per 20-hour window, filters against the tracked technology names, and applies a local publication-date watermark with a one-day overlap. Coverage is remote-only, listings are delayed by the provider, and stored job evidence must retain its Remotive URL and attribution.

### GitHub Actions scheduling

The scheduled workflow is in `.github/workflows/signal-ingestion.yml`. It expects the repository secrets `SIGNAL_DATABASE_URL` and `SIGNAL_GITHUB_TOKEN`. The workflow is prepared on the implementation branch, but GitHub runs scheduled workflows only when the workflow file exists on the default branch. Since `main` is intentionally untouched, the cloud schedule will not activate until a future, explicitly approved merge. Local manual execution remains available now.

## Run tests

```powershell
cd F:\SIGNAL
.\backend\venv\Scripts\Activate.ps1
$env:PYTHONPATH = "$PWD\backend"
python -m unittest discover -s tests -p "test_*.py" -v
```
