# Contributing

Thanks for contributing to `prediction-market-data-pipeline`.

This repository is centered on one problem: collecting and validating BTC 5-minute Polymarket market data, then turning that data into research-ready datasets and backtests. Changes should preserve that focus.

## Before You Start

- read [README.md](README.md)
- read the operational docs in [PROJECT_MANAGEMENT](PROJECT_MANAGEMENT)
- avoid widening repo scope unless the change clearly improves the BTC5M data pipeline

## Development Environment

Recommended local environment:

- Windows 10 or 11
- Python 3.11
- PowerShell

Basic setup:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item polymarket_scanner\.env.example polymarket_scanner\.env
python scripts\btc5m_verify_setup.py
```

## Contribution Rules

- keep runtime artifacts out of Git
- never commit real `.env` files, database files, logs, lock files, or backups
- do not commit secrets, API keys, certificates, or private keys
- prefer repo-relative paths over machine-specific absolute paths
- preserve Windows-first operational behavior unless the change intentionally improves cross-platform support
- keep changes focused and reversible

## Validation Expectations

For non-trivial changes, include the validation you ran.

Common checks:

```powershell
python scripts\btc5m_verify_setup.py
python -m py_compile scripts\btc5m_verify_setup.py
python scripts\btc5m_collection_summary.py --json
```

If your change touches live collection behavior, say whether you tested:

- collector start/stop/status flow
- setup verification
- health check / summary behavior
- scheduler registration
- backup generation

## Pull Requests

Good pull requests should:

- explain the problem clearly
- keep scope narrow
- mention operational impact
- mention any migration or config changes
- include validation notes

Examples of useful contribution areas:

- data quality improvements
- better audit logic
- safer operational tooling
- clearer docs and onboarding
- backtest correctness improvements

Examples that need especially strong justification:

- broad architectural rewrites
- unrelated market support
- changes that weaken observability or safety checks

## Style

- keep code readable and direct
- prefer explicit behavior over clever abstractions
- preserve existing operator-facing naming where practical
- add comments only when they clarify non-obvious behavior

## Questions

If a proposed change affects live collection safety, runtime paths, scheduler behavior, or data integrity, open an issue or discussion before making a broad refactor.
