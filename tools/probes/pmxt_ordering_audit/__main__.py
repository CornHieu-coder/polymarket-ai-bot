"""CLI for the bounded offline IP-002 audit."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .download import (
    AUTHORIZED_OUTPUT_ROOT,
    DownloadFailure,
    acquire_samples,
    ensure_authorized_output,
)
from .engine import (
    analyze_file,
    atomic_write_json,
    pool_results,
    runtime_provenance,
)
from .report import render_report, write_report
from .core import sha256_file
from .recovery import (
    _atomic_create_json,
    analyze_shard,
    partition_sample,
    run_pilot,
    select_august_sample,
)
from .recovery_run import complete_recovery, finalize_recovery
from .streaming import (
    analyze_stream,
    compare_preserved_checkpoint,
    run_streaming_pilot,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = AUTHORIZED_OUTPUT_ROOT / "ip-002-predeclared"
REPORT_PATH = REPO_ROOT / "docs" / "experiments" / "pmxt-ordering-ambiguity-audit.md"
OFFLINE_TEST_COMMAND = (
    "python -m unittest discover -s tests/probes -p 'test_*.py' -v"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IP-002 public-download and offline pmxt ordering audit"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    download = subparsers.add_parser("download")
    download.add_argument("--output-directory", type=Path, default=DEFAULT_OUTPUT)
    analyze = subparsers.add_parser("analyze")
    analyze.add_argument("--output-directory", type=Path, default=DEFAULT_OUTPUT)
    analyze.add_argument("--offline-test-result", required=True)
    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--output-directory", type=Path, default=DEFAULT_OUTPUT)
    finalize.add_argument(
        "--feasibility-label",
        required=True,
        choices=(
            "PRACTICALLY_LOW_COST",
            "MATERIAL_COVERAGE_COST",
            "SEVERE_COVERAGE_COST",
            "UNRESOLVED",
        ),
    )
    finalize.add_argument("--feasibility-rationale", required=True)
    partition_worker = subparsers.add_parser("recovery-partition-worker")
    partition_worker.add_argument("--original-output", type=Path, required=True)
    partition_worker.add_argument("--recovery-root", type=Path, required=True)
    partition_worker.add_argument("--shard-count", type=int, choices=(32, 64), required=True)
    shard_worker = subparsers.add_parser("recovery-shard-worker")
    shard_worker.add_argument("--original-output", type=Path, required=True)
    shard_worker.add_argument("--partition-manifest", type=Path, required=True)
    shard_worker.add_argument("--shard-id", type=int, required=True)
    shard_worker.add_argument("--checkpoint-directory", type=Path, required=True)
    shard_worker.add_argument("--repository", type=Path, required=True)
    pilot = subparsers.add_parser("recovery-pilot")
    pilot.add_argument("--original-output", type=Path, default=DEFAULT_OUTPUT)
    pilot.add_argument(
        "--recovery-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002r-recovery",
    )
    pilot.add_argument("--repository", type=Path, default=REPO_ROOT)
    pilot.add_argument("--shard-count", type=int, choices=(32, 64), default=32)
    pilot.add_argument("--offline-test-result", required=True)
    complete = subparsers.add_parser("recovery-complete")
    complete.add_argument("--original-output", type=Path, default=DEFAULT_OUTPUT)
    complete.add_argument(
        "--recovery-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002r-recovery",
    )
    complete.add_argument("--repository", type=Path, default=REPO_ROOT)
    complete.add_argument("--offline-test-result", required=True)
    recovery_finalize = subparsers.add_parser("recovery-finalize")
    recovery_finalize.add_argument(
        "--recovery-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002r-recovery",
    )
    recovery_finalize.add_argument("--repository", type=Path, default=REPO_ROOT)
    recovery_finalize.add_argument("--report-path", type=Path, default=REPORT_PATH)
    recovery_finalize.add_argument(
        "--feasibility-label",
        required=True,
        choices=(
            "PRACTICALLY_LOW_COST",
            "MATERIAL_COVERAGE_COST",
            "SEVERE_COVERAGE_COST",
            "UNRESOLVED",
        ),
    )
    recovery_finalize.add_argument("--feasibility-rationale", required=True)
    recovery_finalize.add_argument("--final-test-result", required=True)
    streaming_worker = subparsers.add_parser("streaming-window-worker")
    streaming_worker.add_argument("--path", type=Path, required=True)
    streaming_worker.add_argument("--actual-hour", required=True)
    streaming_worker.add_argument("--mode", choices=("W1", "W2"), required=True)
    streaming_worker.add_argument("--output", type=Path, required=True)
    streaming_compare = subparsers.add_parser("streaming-compare-checkpoint")
    streaming_compare.add_argument("--original-output", type=Path, default=DEFAULT_OUTPUT)
    streaming_compare.add_argument(
        "--recovery-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002r-recovery",
    )
    streaming_compare.add_argument("--output", type=Path, required=True)
    streaming_pilot = subparsers.add_parser("streaming-pilot")
    streaming_pilot.add_argument("--original-output", type=Path, default=DEFAULT_OUTPUT)
    streaming_pilot.add_argument(
        "--recovery-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002r-recovery",
    )
    streaming_pilot.add_argument(
        "--pilot-root",
        type=Path,
        default=AUTHORIZED_OUTPUT_ROOT / "ip-002s-streaming-pilot",
    )
    streaming_pilot.add_argument("--repository", type=Path, default=REPO_ROOT)
    streaming_pilot.add_argument("--offline-test-result", required=True)
    streaming_pilot.add_argument(
        "--exact-equivalence-passed",
        action="store_true",
        help="Record that the complete offline exact-equivalence suite passed.",
    )
    streaming_pilot.add_argument(
        "--real-shard-comparison",
        type=Path,
        required=True,
    )
    return parser


def _load_download_provenance(output_directory: Path) -> dict:
    path = output_directory / "download-provenance.json"
    if not path.is_file():
        raise FileNotFoundError(f"download provenance does not exist: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("valid_sample_count", 0) < 2:
        raise DownloadFailure("download provenance contains fewer than two valid dates")
    return value


def _analyze(arguments: argparse.Namespace) -> int:
    output_directory = ensure_authorized_output(arguments.output_directory)
    download = _load_download_provenance(output_directory)
    started_at = _utc_now()
    runtime = runtime_provenance(REPO_ROOT)
    if runtime["git"]["dirty"]:
        raise RuntimeError(
            "analysis requires a clean committed audit implementation for provenance"
        )
    analyzed: list[dict] = []
    failures: list[dict] = []
    working_directory = output_directory / "work"
    for sample in download["samples"]:
        if sample.get("status") != "VALID":
            failures.append(
                {
                    "requested_hour": sample["requested_hour"],
                    "stage": "selection",
                    "reason": sample.get("failure_reason", "sample unavailable"),
                    "attempts": sample.get("attempts", []),
                }
            )
            continue
        try:
            file_result = analyze_file(sample, working_directory=working_directory)
            file_result["provenance"].update(
                {
                    "audit_code_git_commit": runtime["git"]["commit"],
                    "python_version": runtime["python"],
                    "duckdb_version": runtime["duckdb"],
                }
            )
            analyzed.append(file_result)
        except Exception as exc:
            failures.append(
                {
                    "requested_hour": sample["requested_hour"],
                    "actual_hour": sample.get("actual_hour"),
                    "stage": "analysis",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
    ended_at = _utc_now()
    analysis_path = output_directory / "analysis-a1-a8.json"
    if len(analyzed) < 2:
        partial = {
            "status": "UNRESOLVED",
            "run": {
                "started_at": started_at,
                "ended_at": ended_at,
                "runtime": runtime,
                "offline_test_command": OFFLINE_TEST_COMMAND,
                "offline_test_result": arguments.offline_test_result,
                "analysis_path": str(analysis_path),
            },
            "download": download,
            "samples": analyzed,
            "failures": failures,
            "deviation_from_ip_002": "None",
        }
        atomic_write_json(analysis_path, partial)
        raise RuntimeError("fewer than two files completed A1-A8; audit is UNRESOLVED")
    analysis = {
        "status": "COMPLETE" if len(analyzed) == 3 else "COMPLETE_WITH_MISSING_SAMPLE",
        "run": {
            "started_at": started_at,
            "ended_at": ended_at,
            "runtime": runtime,
            "offline_test_command": OFFLINE_TEST_COMMAND,
            "offline_test_result": arguments.offline_test_result,
            "output_directory": str(output_directory),
            "analysis_path": str(analysis_path),
        },
        "download": download,
        "samples": analyzed,
        "failures": failures,
        "deviation_from_ip_002": "None",
        "safety": {
            "public_pmxt_archive_get_only": True,
            "authenticated_polymarket_calls": 0,
            "wallet_or_credentials_used": False,
            "orders_or_trading_operations": 0,
            "strategy_fill_pnl_or_forecast_simulation": False,
            "production_replay_implemented": False,
        },
    }
    atomic_write_json(analysis_path, analysis)
    analysis_hash = sha256_file(analysis_path)
    print(json.dumps({"analysis": str(analysis_path), "sha256": analysis_hash}, indent=2))
    return 0


def _finalize(arguments: argparse.Namespace) -> int:
    output_directory = ensure_authorized_output(arguments.output_directory)
    analysis_path = output_directory / "analysis-a1-a8.json"
    if not analysis_path.is_file():
        raise FileNotFoundError(f"A1-A8 analysis does not exist: {analysis_path}")
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    if len(analysis.get("samples", [])) < 2:
        raise RuntimeError("fewer than two samples completed A1-A8")
    summary_path = output_directory / "summary.json"
    pooled = pool_results(
        analysis["samples"],
        feasibility_label=arguments.feasibility_label,
        feasibility_rationale=arguments.feasibility_rationale,
    )
    summary = {
        **analysis,
        "pooled": pooled,
        "run": {
            **analysis["run"],
            "analysis_sha256": sha256_file(analysis_path),
            "finalized_at": _utc_now(),
            "summary_path": str(summary_path),
        },
    }
    atomic_write_json(summary_path, summary)
    summary_hash = sha256_file(summary_path)
    write_report(REPORT_PATH, render_report(summary, summary_sha256=summary_hash))
    print(json.dumps({"summary": str(summary_path), "sha256": summary_hash}, indent=2))
    return 0


def main() -> int:
    arguments = _parser().parse_args()
    if arguments.command == "download":
        result = acquire_samples(arguments.output_directory)
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.command == "analyze":
        return _analyze(arguments)
    if arguments.command == "recovery-partition-worker":
        sample = select_august_sample(arguments.original_output)
        partition_sample(sample, arguments.recovery_root, arguments.shard_count)
        return 0
    if arguments.command == "recovery-shard-worker":
        sample = select_august_sample(arguments.original_output)
        analyze_shard(
            sample,
            arguments.partition_manifest,
            arguments.shard_id,
            arguments.checkpoint_directory,
            arguments.repository,
        )
        return 0
    if arguments.command == "recovery-pilot":
        result = run_pilot(
            arguments.original_output,
            arguments.recovery_root,
            arguments.repository,
            arguments.offline_test_result,
            arguments.shard_count,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.command == "recovery-complete":
        result = complete_recovery(
            original_output=arguments.original_output,
            recovery_root=arguments.recovery_root,
            repository=arguments.repository,
            offline_test_result=arguments.offline_test_result,
        )
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "samples": [item["actual_hour"] for item in result["samples"]],
                    "august_recovery": result["recovery"]["august_recovery"],
                    "june_equivalence": result["recovery"]["june_equivalence"],
                },
                indent=2,
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "recovery-finalize":
        result = finalize_recovery(
            recovery_root=arguments.recovery_root,
            repository=arguments.repository,
            report_path=arguments.report_path,
            feasibility_label=arguments.feasibility_label,
            feasibility_rationale=arguments.feasibility_rationale,
            final_test_result=arguments.final_test_result,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.command == "streaming-window-worker":
        result = analyze_stream(
            arguments.path,
            actual_hour=arguments.actual_hour,
            mode=arguments.mode,
        )
        _atomic_create_json(arguments.output, result)
        return 0
    if arguments.command == "streaming-compare-checkpoint":
        result = compare_preserved_checkpoint(
            original_output=arguments.original_output,
            recovery_root=arguments.recovery_root,
        )
        _atomic_create_json(arguments.output, result)
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.command == "streaming-pilot":
        comparison = json.loads(
            arguments.real_shard_comparison.read_text(encoding="utf-8")
        )
        result = run_streaming_pilot(
            original_output=arguments.original_output,
            recovery_root=arguments.recovery_root,
            pilot_root=arguments.pilot_root,
            repository=arguments.repository,
            offline_test_result=arguments.offline_test_result,
            exact_equivalence_passed=arguments.exact_equivalence_passed,
            real_shard_comparison=comparison,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    return _finalize(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
