# Repository Guidelines

## Project Structure & Module Organization

This is a Python 3.10 Flask service that migrates OpenStack VMs across clouds by
copying Ceph RBD volumes. Runtime logic lives in top-level modules: `app.py`
(Flask routes and job wiring), `openstack_utils.py` and `ceph_utils.py`
(cloud/RBD clients), `migration_manager.py`, `migration_planner.py`,
`job_manager.py`, `state_machine.py`, `graceful_shutdown.py`,
`excel_parser.py`, plus `config.py` and `repro_create.py`. The UI is
`templates/index.html` with vendored assets in `static/`. Tests are in `tests/`;
design specs and implementation plans are in `docs/superpowers/`. Treat
`uploads/` (runtime state) and `__pycache__/` as generated, not source.

## Build, Test, and Development Commands

- `pip install -r requirements.txt` — install dependencies.
- `python app.py` — run the service at `http://localhost:19099/`.
- `python3 -m unittest discover -s tests -v` — run the full test suite.
- `python3 -m unittest tests.test_openstack_utils` — run a single module.
- `docker build -t vm-migrate .` — build the image (bundles a pinned Ceph
  Nautilus 14.2.22 `rbd`, matching the source/target clusters; override the
  deb mirror with `--build-arg CEPH_DEB_REPO=...`).
- `docker buildx build --platform linux/amd64,linux/arm64 --push -t <tag> .` —
  the Dockerfile is architecture-aware (`TARGETARCH`), so amd64 and arm64 share
  one multi-arch tag; pip downloads can be redirected with
  `--build-arg PIP_INDEX_URL=...`.
- `python repro_create.py --auth-url ...` — reproduce BFV create/quota issues.

## Coding Style & Naming Conventions

Follow PEP 8: 4-space indentation, `snake_case` functions and variables,
`PascalCase` classes, `UPPER_CASE` module constants. Keep type hints (PEP 604
unions such as `dict[str, Any] | None`) and short docstrings on public
functions. Mark intentional lint findings inline with a reason, e.g.
`# noqa: BLE001 - keep VM-level failures isolated`. No formatter config is
committed, so match surrounding code.

## Testing Guidelines

Tests use the standard `unittest` framework with `unittest.mock`; no external
coverage threshold is enforced. Name files `test_<module>.py`, classes
`<Behavior>Test`, and methods `test_<expected_behavior>`. Mock the OpenStack and
Ceph clients instead of reaching real clouds, keep cases deterministic, and run
the full suite before opening a PR.

## Commit & Pull Request Guidelines

This checkout has no Git history, so follow Conventional Commits (`feat:`,
`fix:`, `docs:`, `test:`) with an imperative subject. PRs should describe the
change, list affected modules, link the relevant spec under
`docs/superpowers/`, state the test commands run, and attach screenshots for UI
changes in `templates/index.html`.

## Configuration & Deployment

Tune behavior with `MIGRATION_*` environment variables
(`MIGRATION_MAX_RBD_COPIES`, `MIGRATION_MEMORY_HIGH_WATER`,
`MIGRATION_COPY_RESERVE_MB`, `MIGRATION_SHUTDOWN_GRACE_SECONDS`,
`MIGRATION_MAX_UPLOAD_MB`, `MIGRATION_UPLOAD_RETENTION_DAYS`). Never commit
credentials or Ceph configs; the app stores them under `uploads/` with `0600`
permissions. Deploy by syncing the `migrate-vm-bin`/`migrate-vm-html` ConfigMaps
and restarting the deployment, keeping a single replica so in-process
`JobManager` state stays visible.
