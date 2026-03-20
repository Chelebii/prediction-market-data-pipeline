"""Non-live verification for a fresh BTC5M repository clone."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_EXAMPLE_PATH = ROOT_DIR / "polymarket_scanner" / ".env.example"
ENV_PATH = ROOT_DIR / "polymarket_scanner" / ".env"

REQUIRED_FILES = (
    ROOT_DIR / "README.md",
    ROOT_DIR / "requirements.txt",
    ROOT_DIR / "control" / "scripts" / "btc5m_collection_control.ps1",
    ROOT_DIR / "control" / "scripts" / "register_btc5m_collection_tasks.ps1",
    ROOT_DIR / "control" / "scripts" / "ensure_btc5m_process_exes.ps1",
    ROOT_DIR / "control" / "scripts" / "start_btc5m_collectors.cmd",
    ROOT_DIR / "polymarket_scanner" / "btc_5min_clob_scanner.py",
    ROOT_DIR / "polymarket_scanner" / ".env.example",
    ROOT_DIR / "scripts" / "btc5m_collection_summary.py",
    ROOT_DIR / "scripts" / "btc5m_healthcheck.py",
    ROOT_DIR / "scripts" / "btc5m_verify_setup.py",
)

RUNTIME_PATH_KEYS = (
    "BTC_5MIN_SNAPSHOT_PATH",
    "BTC5M_REFERENCE_LOG_PATH",
    "BTC5M_REFERENCE_LOCK_PATH",
    "BTC5M_RESOLUTION_LOG_PATH",
    "BTC5M_RESOLUTION_LOCK_PATH",
    "BTC5M_HEALTH_LOG_PATH",
    "BTC5M_HEALTH_STATUS_PATH",
    "BTC5M_AUDIT_LOG_PATH",
    "BTC5M_AUDIT_LOCK_PATH",
    "BTC5M_BACKUP_DIR",
    "BTC5M_BACKUP_LOG_PATH",
    "BTC5M_BACKUP_LATEST_METADATA_PATH",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify a fresh BTC5M repository clone without starting collectors.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a text report.")
    return parser.parse_args()


def add_check(results: list[dict[str, Any]], name: str, status: str, detail: str) -> None:
    results.append({"name": name, "status": status, "detail": detail})


def is_relative_repo_path(value: str) -> bool:
    if not value:
        return False
    text = str(value).strip()
    if not text:
        return False
    return not Path(text).is_absolute()


def build_results() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    if sys.version_info >= (3, 11):
        add_check(results, "python_version", "PASS", f"Python {platform.python_version()} detected.")
    else:
        add_check(results, "python_version", "FAIL", f"Python 3.11+ required, found {platform.python_version()}.")

    if os.name == "nt":
        add_check(results, "platform", "PASS", "Windows platform detected.")
    else:
        add_check(results, "platform", "WARN", f"Windows-first repo; detected platform is {platform.system()}.")

    for module_name in ("requests", "dotenv"):
        try:
            __import__(module_name)
            add_check(results, f"dependency:{module_name}", "PASS", f"Import succeeded for {module_name}.")
        except Exception as exc:
            add_check(results, f"dependency:{module_name}", "FAIL", f"Import failed for {module_name}: {exc}")

    for path in REQUIRED_FILES:
        if path.exists():
            add_check(results, f"file:{path.relative_to(ROOT_DIR)}", "PASS", "Required file is present.")
        else:
            add_check(results, f"file:{path.relative_to(ROOT_DIR)}", "FAIL", "Required file is missing.")

    if ENV_EXAMPLE_PATH.exists():
        env_example = dotenv_values(ENV_EXAMPLE_PATH)
        add_check(results, "env_example", "PASS", "Example environment file is present and readable.")

        secret_like_keys = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
        populated_keys = [key for key in secret_like_keys if str(env_example.get(key) or "").strip()]
        if populated_keys:
            add_check(
                results,
                "env_example_secrets",
                "FAIL",
                f".env.example should not contain populated secret values: {', '.join(populated_keys)}",
            )
        else:
            add_check(results, "env_example_secrets", "PASS", ".env.example does not ship with populated secrets.")

        bad_path_keys = [key for key in RUNTIME_PATH_KEYS if not is_relative_repo_path(str(env_example.get(key) or ""))]
        if bad_path_keys:
            add_check(
                results,
                "env_example_paths",
                "FAIL",
                f"Expected repo-relative runtime paths in .env.example, but found invalid values for: {', '.join(bad_path_keys)}",
            )
        else:
            add_check(results, "env_example_paths", "PASS", ".env.example runtime paths are repo-relative.")
    else:
        add_check(results, "env_example", "FAIL", "polymarket_scanner/.env.example is missing.")

    if ENV_PATH.exists():
        add_check(results, "local_env", "PASS", "Local polymarket_scanner/.env exists.")
    else:
        add_check(results, "local_env", "WARN", "Local polymarket_scanner/.env not found; copy from .env.example before live runs.")

    runtime_path = ROOT_DIR / "runtime"
    state_path = ROOT_DIR / "state"
    add_check(results, "runtime_dir", "INFO", f"Runtime artifacts will be written under {runtime_path}.")
    add_check(results, "state_dir", "INFO", f"State artifacts will be written under {state_path}.")

    return results


def print_text(results: list[dict[str, Any]]) -> None:
    for item in results:
        print(f"{item['status']:<4} {item['name']}: {item['detail']}")

    failures = sum(1 for item in results if item["status"] == "FAIL")
    warnings = sum(1 for item in results if item["status"] == "WARN")
    print("")
    print(f"Summary: {failures} failure(s), {warnings} warning(s)")
    if failures:
        print("Setup verification failed. Fix the failing checks before live collection.")
    elif warnings:
        print("Setup verification passed with warnings. Review them before unattended live collection.")
    else:
        print("Setup verification passed.")


def main() -> int:
    args = parse_args()
    results = build_results()
    failures = [item for item in results if item["status"] == "FAIL"]
    warnings = [item for item in results if item["status"] == "WARN"]

    payload = {
        "ok": not failures,
        "warning_count": len(warnings),
        "failure_count": len(failures),
        "results": results,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print_text(results)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
