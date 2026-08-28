"""One-time, fail-closed authorization boundary for the full August IP-002S scan.

This module launches only the frozen streaming A1-A8 implementation.  It does not
pool samples, evaluate A9, finalize a report, or expose a general large-file scan.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pyarrow

from .core import normalize_json, sha256_file
from .download import AUTHORIZED_OUTPUT_ROOT
from .engine import git_provenance
from .recovery import _atomic_create_json
from .streaming import (
    ARROW_BATCH_SIZE,
    _analyze_authorized_full_august,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
ORIGINAL_OUTPUT = AUTHORIZED_OUTPUT_ROOT / "ip-002-predeclared"
FULL_AUGUST_OUTPUT = AUTHORIZED_OUTPUT_ROOT / "ip-002s-full-august"
EXPECTED_AUGUST_HOUR = "2026-08-01T12"
EXPECTED_AUGUST_OBJECT = "polymarket_orderbook_2026-08-01T12.parquet"
EXPECTED_AUGUST_SHA256 = (
    "471db57da81342f7bc67897881bc0367799fa5eafc1bd1f5e04c0ab2864b3093"
)
OWNER_APPROVAL_PHRASE = "OWNER_APPROVED_IP002S_FULL_AUGUST_8_HOURS"
AUTHORIZED_WALL_SECONDS = 8 * 60 * 60
PROGRESS_INTERVAL_SECONDS = 30.0
AUTHORIZATION_FORMAT = "ip-002s-full-august-authorization-v1"
RESULT_FORMAT = "ip-002s-full-august-a1-a8-v1"


class FullAugustAuthorizationError(RuntimeError):
    """Raised when the one-time full-August authorization boundary is not met."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _runtime_provenance(repository: Path) -> dict[str, Any]:
    return {
        "git": git_provenance(repository),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": sys.executable,
        "pyarrow_version": pyarrow.__version__,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "hostname": platform.node(),
        "logical_cpu_count": os.cpu_count(),
        "process_id": os.getpid(),
        "working_directory": str(repository.resolve()),
    }


def _require_clean_committed_git(
    repository: Path, *, expected_commit: str | None = None
) -> dict[str, Any]:
    git = git_provenance(repository)
    if git["dirty"]:
        raise FullAugustAuthorizationError(
            "full-August streaming requires a clean committed worktree"
        )
    if not git.get("commit"):
        raise FullAugustAuthorizationError("Git commit provenance is unavailable")
    if expected_commit is not None and git["commit"] != expected_commit:
        raise FullAugustAuthorizationError(
            f"Git head changed: expected {expected_commit}, observed {git['commit']}"
        )
    return git


def _load_preserved_august(original_output: Path) -> dict[str, Any]:
    provenance_path = original_output / "download-provenance.json"
    if not provenance_path.is_file():
        raise FullAugustAuthorizationError(
            f"preserved download provenance is missing: {provenance_path}"
        )
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    matches = [
        item
        for item in provenance.get("samples", [])
        if item.get("requested_hour") == EXPECTED_AUGUST_HOUR
    ]
    if len(matches) != 1:
        raise FullAugustAuthorizationError(
            "preserved provenance must contain exactly one predeclared August sample"
        )
    sample = matches[0]
    required = {
        "status": "VALID",
        "actual_hour": EXPECTED_AUGUST_HOUR,
        "requested_object_key": EXPECTED_AUGUST_OBJECT,
        "actual_object_key": EXPECTED_AUGUST_OBJECT,
        "sha256": EXPECTED_AUGUST_SHA256,
    }
    differences = {
        key: {"expected": value, "observed": sample.get(key)}
        for key, value in required.items()
        if sample.get(key) != value
    }
    if differences:
        raise FullAugustAuthorizationError(
            f"preserved August provenance is not the authorized sample: {differences}"
        )
    raw_path = (original_output / "raw" / EXPECTED_AUGUST_OBJECT).resolve()
    expected_path = (ORIGINAL_OUTPUT / "raw" / EXPECTED_AUGUST_OBJECT).resolve()
    if original_output.resolve() == ORIGINAL_OUTPUT.resolve() and raw_path != expected_path:
        raise FullAugustAuthorizationError("resolved August sample path is not canonical")
    if not raw_path.is_file():
        raise FullAugustAuthorizationError(
            f"preserved August Parquet is missing: {raw_path}"
        )
    actual_hash = sha256_file(raw_path)
    if actual_hash != EXPECTED_AUGUST_SHA256:
        raise FullAugustAuthorizationError(
            f"preserved August raw hash mismatch: {actual_hash}"
        )
    actual_bytes = raw_path.stat().st_size
    if actual_bytes != int(sample.get("byte_length", -1)):
        raise FullAugustAuthorizationError(
            "preserved August byte length differs from download provenance"
        )
    return {
        "requested_hour": EXPECTED_AUGUST_HOUR,
        "actual_hour": EXPECTED_AUGUST_HOUR,
        "object_key": EXPECTED_AUGUST_OBJECT,
        "local_path": str(raw_path),
        "sha256": actual_hash,
        "byte_length": actual_bytes,
        "recorded_row_count": next(
            (
                int(attempt["row_count"])
                for attempt in sample.get("attempts", [])
                if attempt.get("outcome") == "DOWNLOADED"
                and attempt.get("row_count") is not None
            ),
            None,
        ),
        "download_provenance_path": str(provenance_path.resolve()),
        "download_provenance_sha256": sha256_file(provenance_path),
        "verified_at": _utc_now(),
    }


def _ensure_unused_output_root(output_root: Path) -> None:
    if output_root.exists():
        existing = sorted(str(item.name) for item in output_root.iterdir())
        if existing:
            raise FileExistsError(
                "full-August output already contains one-time run artifacts: "
                + ", ".join(existing)
            )


def _run_paths(output_root: Path) -> dict[str, Path]:
    return {
        "authorization": output_root / "authorization.json",
        "process": output_root / "process.json",
        "worker_claim": output_root / "worker-claim.json",
        "progress": output_root / "progress.json",
        "log": output_root / "full-august.log",
        "result": output_root / "full-august-a1-a8.json",
        "failure": output_root / "failure.json",
    }


def _atomic_replace_progress(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.part-{os.getpid()}-{uuid.uuid4().hex}")
    payload = json.dumps(
        normalize_json(value), indent=2, ensure_ascii=False, sort_keys=True
    ) + "\n"
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _detached_command(authorization_path: Path, run_id: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "tools.probes.pmxt_ordering_audit",
        "streaming-full-august-worker",
        "--authorization",
        str(authorization_path.resolve()),
        "--run-id",
        run_id,
    ]


def _spawn_detached(
    command: list[str], *, repository: Path, log_path: Path
) -> subprocess.Popen[Any]:
    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    else:
        start_new_session = True
    with log_path.open("x", encoding="utf-8", newline="\n") as log_handle:
        return subprocess.Popen(
            command,
            cwd=repository,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            close_fds=True,
            creationflags=creationflags,
            start_new_session=start_new_session,
        )


def launch_full_august(
    *, expected_git_sha: str, owner_approval: str, repository: Path = REPO_ROOT
) -> dict[str, Any]:
    """Consume the one-time authorization and launch the detached A1-A8 worker."""

    if owner_approval != OWNER_APPROVAL_PHRASE:
        raise FullAugustAuthorizationError(
            "the exact owner-approved eight-hour authorization phrase is required"
        )
    if repository.resolve() != REPO_ROOT.resolve():
        raise FullAugustAuthorizationError("the launcher accepts only this repository")
    if ORIGINAL_OUTPUT.resolve() != (
        AUTHORIZED_OUTPUT_ROOT / "ip-002-predeclared"
    ).resolve():
        raise FullAugustAuthorizationError("original evidence path is not canonical")
    if FULL_AUGUST_OUTPUT.resolve() != (
        AUTHORIZED_OUTPUT_ROOT / "ip-002s-full-august"
    ).resolve():
        raise FullAugustAuthorizationError("full-August output path is not canonical")
    git = _require_clean_committed_git(
        repository, expected_commit=expected_git_sha
    )
    _ensure_unused_output_root(FULL_AUGUST_OUTPUT)
    evidence = _load_preserved_august(ORIGINAL_OUTPUT)
    FULL_AUGUST_OUTPUT.mkdir(parents=True, exist_ok=True)
    paths = _run_paths(FULL_AUGUST_OUTPUT)
    run_id = uuid.uuid4().hex
    authorized_at = _utc_now()
    runtime = _runtime_provenance(repository)
    if runtime["git"] != git:
        raise FullAugustAuthorizationError(
            "Git provenance changed while preparing the authorization"
        )
    authorization = {
        "format": AUTHORIZATION_FORMAT,
        "run_id": run_id,
        "authorized_at": authorized_at,
        "owner_approval": OWNER_APPROVAL_PHRASE,
        "authorized_wall_clock_seconds": AUTHORIZED_WALL_SECONDS,
        "operational_change_note": (
            "Later owner-approved eight-hour operational budget; this does not "
            "reinterpret the original IP-002S 30-minute feasibility gate."
        ),
        "input": evidence,
        "runtime_provenance": runtime,
        "paths": {key: str(value.resolve()) for key, value in paths.items()},
        "scope": {
            "frozen_ip002s_streaming_a1_a8_only": True,
            "a9_invoked": False,
            "report_finalization_invoked": False,
            "adr_or_production_replay_invoked": False,
            "strategy_risk_execution_or_forecasting_invoked": False,
        },
    }
    _atomic_create_json(paths["authorization"], authorization)
    command = _detached_command(paths["authorization"], run_id)
    process = _spawn_detached(command, repository=repository, log_path=paths["log"])
    process_record = {
        "format": "ip-002s-full-august-process-v1",
        "run_id": run_id,
        "pid": process.pid,
        "launched_at": _utc_now(),
        "authorization_path": str(paths["authorization"].resolve()),
        "worker_claim_path": str(paths["worker_claim"].resolve()),
        "log_path": str(paths["log"].resolve()),
        "progress_path": str(paths["progress"].resolve()),
        "result_path": str(paths["result"].resolve()),
        "failure_path": str(paths["failure"].resolve()),
        "git_sha": git["commit"],
        "authorized_wall_clock_seconds": AUTHORIZED_WALL_SECONDS,
    }
    _atomic_create_json(paths["process"], process_record)
    return process_record


def _validated_authorization(
    authorization_path: Path, *, run_id: str, repository: Path
) -> tuple[dict[str, Any], dict[str, Path]]:
    expected_authorization = (FULL_AUGUST_OUTPUT / "authorization.json").resolve()
    if authorization_path.resolve() != expected_authorization:
        raise FullAugustAuthorizationError(
            "worker authorization path is outside the canonical full-August output"
        )
    value = json.loads(authorization_path.read_text(encoding="utf-8"))
    if value.get("format") != AUTHORIZATION_FORMAT:
        raise FullAugustAuthorizationError("unexpected authorization format")
    if value.get("run_id") != run_id:
        raise FullAugustAuthorizationError("worker run ID differs from authorization")
    if value.get("owner_approval") != OWNER_APPROVAL_PHRASE:
        raise FullAugustAuthorizationError("owner authorization is absent")
    if value.get("authorized_wall_clock_seconds") != AUTHORIZED_WALL_SECONDS:
        raise FullAugustAuthorizationError("worker budget is not exactly eight hours")
    paths = _run_paths(FULL_AUGUST_OUTPUT)
    expected_paths = {key: str(path.resolve()) for key, path in paths.items()}
    if value.get("paths") != expected_paths:
        raise FullAugustAuthorizationError("authorized output paths were altered")
    authorized_commit = value.get("runtime_provenance", {}).get("git", {}).get(
        "commit"
    )
    _require_clean_committed_git(repository, expected_commit=authorized_commit)
    if paths["result"].exists():
        raise FileExistsError(
            f"full-August result already exists and will not be overwritten: {paths['result']}"
        )
    if paths["failure"].exists():
        raise FileExistsError(
            "a prior full-August failure record exists; silent restart is forbidden"
        )
    return value, paths


def _final_result(
    *,
    authorization: Mapping[str, Any],
    analysis: Mapping[str, Any],
    started_at: str,
    ended_at: str,
    elapsed_seconds: float,
    worker_provenance: Mapping[str, Any],
    input_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "format": RESULT_FORMAT,
        "status": "COMPLETE",
        "run_id": authorization["run_id"],
        "started_at": started_at,
        "ended_at": ended_at,
        "elapsed_seconds": elapsed_seconds,
        "authorized_wall_clock_seconds": AUTHORIZED_WALL_SECONDS,
        "input": input_evidence,
        "worker_runtime_provenance": worker_provenance,
        "streaming": analysis["streaming"],
        "a1_a8": analysis["scientific"],
        "scope": {
            "frozen_ip002s_streaming_a1_a8_only": True,
            "a9_invoked": False,
            "report_finalization_invoked": False,
            "adr_or_production_replay_invoked": False,
            "strategy_risk_execution_or_forecasting_invoked": False,
        },
    }


def run_full_august_worker(
    *, authorization_path: Path, run_id: str, repository: Path = REPO_ROOT
) -> dict[str, Any]:
    """Run one authorized full-August scan and atomically create its A1-A8 result."""

    worker_started_monotonic = time.monotonic()
    started_at = _utc_now()
    authorization, paths = _validated_authorization(
        authorization_path, run_id=run_id, repository=repository
    )
    expected_commit = authorization["runtime_provenance"]["git"]["commit"]
    _atomic_create_json(
        paths["worker_claim"],
        {
            "format": "ip-002s-full-august-worker-claim-v1",
            "run_id": run_id,
            "pid": os.getpid(),
            "claimed_at": started_at,
            "git_sha": expected_commit,
            "authorization_path": str(authorization_path.resolve()),
        },
    )
    deadline = worker_started_monotonic + AUTHORIZED_WALL_SECONDS
    last_progress = 0.0

    def progress(update: Mapping[str, Any]) -> None:
        nonlocal last_progress
        now = time.monotonic()
        if now - last_progress < PROGRESS_INTERVAL_SECONDS:
            return
        last_progress = now
        record = {
            "format": "ip-002s-full-august-progress-v1",
            "status": "RUNNING",
            "run_id": run_id,
            "pid": os.getpid(),
            "started_at": started_at,
            "observed_at": _utc_now(),
            "elapsed_seconds": now - worker_started_monotonic,
            "authorized_wall_clock_seconds": AUTHORIZED_WALL_SECONDS,
            **dict(update),
        }
        _atomic_replace_progress(paths["progress"], record)
        print(json.dumps(normalize_json(record), sort_keys=True), flush=True)

    initial = {
        "batches_read": 0,
        "batches_with_analyzed_rows": 0,
        "physical_rows_read": 0,
        "rows_analyzed": 0,
        "parquet_total_rows": authorization["input"].get("recorded_row_count"),
        "maximum_buffered_logical_group_size": 0,
        "maximum_asset_rows_observed": 0,
    }
    try:
        progress(initial)
        evidence = _load_preserved_august(ORIGINAL_OUTPUT)
        if evidence["sha256"] != authorization["input"]["sha256"]:
            raise FullAugustAuthorizationError(
                "worker input differs from the authorized raw hash"
            )
        worker_provenance = _runtime_provenance(repository)
        if worker_provenance["git"]["commit"] != expected_commit:
            raise FullAugustAuthorizationError("worker Git SHA differs from authorization")
        analysis = _analyze_authorized_full_august(
            Path(evidence["local_path"]),
            actual_hour=EXPECTED_AUGUST_HOUR,
            batch_size=ARROW_BATCH_SIZE,
            deadline_monotonic=deadline,
            progress_callback=progress,
        )
        if time.monotonic() >= deadline:
            raise FullAugustAuthorizationError(
                "authorized eight-hour wall-clock budget expired before completion"
            )
        final_git = _require_clean_committed_git(
            repository, expected_commit=expected_commit
        )
        final_evidence = _load_preserved_august(ORIGINAL_OUTPUT)
        if final_evidence["sha256"] != evidence["sha256"]:
            raise FullAugustAuthorizationError("August raw hash changed during the run")
        worker_provenance = {**worker_provenance, "final_git": final_git}
        ended_at = _utc_now()
        result = _final_result(
            authorization=authorization,
            analysis=analysis,
            started_at=started_at,
            ended_at=ended_at,
            elapsed_seconds=time.monotonic() - worker_started_monotonic,
            worker_provenance=worker_provenance,
            input_evidence=final_evidence,
        )
        _atomic_replace_progress(
            paths["progress"],
            {
                "format": "ip-002s-full-august-progress-v1",
                "status": "FINALIZING",
                "run_id": run_id,
                "pid": os.getpid(),
                "started_at": started_at,
                "observed_at": ended_at,
                "elapsed_seconds": result["elapsed_seconds"],
                "result_path": str(paths["result"].resolve()),
            },
        )
        print(json.dumps({"status": "FINALIZING", "result": str(paths["result"])}), flush=True)
        # The immutable result is the final fallible operation.  A killed or failed
        # worker therefore cannot leave a non-atomic file that looks complete.
        _atomic_create_json(paths["result"], result)
        return result
    except Exception as exc:
        failure = {
            "format": "ip-002s-full-august-failure-v1",
            "status": "FAILED_CLOSED",
            "run_id": run_id,
            "pid": os.getpid(),
            "started_at": started_at,
            "failed_at": _utc_now(),
            "elapsed_seconds": time.monotonic() - worker_started_monotonic,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "result_created": paths["result"].exists(),
        }
        if not paths["failure"].exists():
            _atomic_create_json(paths["failure"], failure)
        raise
