"""Bounded IP-002R completion, cross-engine validation, and finalization."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping

import duckdb

from .core import normalize_json, sha256_file
from .engine import analyze_file
from .recovery import (
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    IP002RClosedError,
    IP_002R_CLOSED_MESSAGE,
    MAX_RSS_BYTES,
    MAX_TEMP_BYTES,
    SHARD_ALGORITHM,
    SOURCE_ROW_ORDINAL,
    _atomic_create_json,
    _canonical_json,
    _directory_size,
    _monitor_process,
    _partition_manifest_path,
    _sql_path,
    _utc_now,
    reduce_checkpoints,
    verify_checkpoint,
    verify_partition,
    verify_preserved_evidence,
)


JUNE_VALIDATION_SHARDS = 1024
MAX_SINGLE_SHARD_SECONDS = 30 * 60


def _validation_shard_sql() -> str:
    material = """
        CASE WHEN market IS NULL THEN 'MN;'
             ELSE 'MB' || octet_length(market)::VARCHAR || ':'
                  || lower(hex(market)) || ';' END
        || CASE WHEN asset_id IS NULL THEN 'AN;'
                ELSE 'AS' || octet_length(encode(asset_id))::VARCHAR || ':'
                     || asset_id || ';' END
    """
    return (
        "mod(('0x' || substr(sha256(" + material
        + "), 1, 16))::UBIGINT, 1024)::INTEGER"
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _immutable_json(path: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_json(value)
    if path.is_file():
        existing = _read_json(path)
        if existing != normalized:
            raise FileExistsError(f"immutable result differs: {path}")
        return existing
    _atomic_create_json(path, normalized)
    return _read_json(path)


def _recovery_blob_sha(repository: Path, revision: str) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", f"{revision}:tools/probes/pmxt_ordering_audit/recovery.py"],
        cwd=repository,
        text=True,
    ).strip()


def _checkpoint_resource_path(recovery_root: Path, shard_id: int) -> Path:
    return recovery_root / "resource-checkpoints-32" / f"shard-{shard_id:03d}.json"


def _validated_pilot(recovery_root: Path, raw_sha256: str) -> dict[str, Any]:
    path = recovery_root / "resource-pilot-32.json"
    pilot = _read_json(path)
    if pilot.get("proceed_gate", {}).get("status") != "PASS":
        raise RuntimeError("the preserved 32-shard pilot did not pass")
    if int(pilot.get("shard_count", 0)) != 32:
        raise ValueError("pilot shard count changed")
    if pilot.get("preserved_evidence", {}).get("samples", [])[-1].get(
        "verified_sha256"
    ) != raw_sha256:
        raise ValueError("pilot belongs to another August raw sample")
    checkpoint_path = recovery_root / "checkpoints-32" / "shard-018.json"
    if sha256_file(checkpoint_path) != pilot["checkpoint_file_sha256"]:
        raise ValueError("pilot checkpoint file hash mismatch")
    verify_checkpoint(checkpoint_path)
    return pilot


def _run_remaining_shards(
    *,
    original_output: Path,
    recovery_root: Path,
    repository: Path,
    sample: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checkpoint_directory = recovery_root / "checkpoints-32"
    worker = [sys.executable, "-m", "tools.probes.pmxt_ordering_audit"]
    resources: list[dict[str, Any]] = []
    for shard in sorted(manifest["shards"], key=lambda item: int(item["shard_id"])):
        shard_id = int(shard["shard_id"])
        checkpoint_path = checkpoint_directory / f"shard-{shard_id:03d}.json"
        if checkpoint_path.is_file():
            checkpoint = verify_checkpoint(checkpoint_path)
            if checkpoint["payload"]["raw_sample_sha256"] != sample["sha256"]:
                raise ValueError(f"checkpoint {shard_id} belongs to another sample")
            if int(checkpoint["payload"]["row_count"]) != int(shard["row_count"]):
                raise ValueError(f"checkpoint {shard_id} row count changed")
            continue
        resource_path = _checkpoint_resource_path(recovery_root, shard_id)
        if resource_path.exists():
            raise FileExistsError(
                f"resource checkpoint exists without analysis checkpoint: {resource_path}"
            )
        monitored = _monitor_process(
            worker
            + [
                "recovery-shard-worker",
                "--original-output",
                str(original_output),
                "--partition-manifest",
                str(_partition_manifest_path(recovery_root, 32)),
                "--shard-id",
                str(shard_id),
                "--checkpoint-directory",
                str(checkpoint_directory),
                "--repository",
                str(repository),
            ],
            checkpoint_directory / "work" / f"shard-{shard_id:03d}",
        )
        if monitored["exit_code"]:
            raise RuntimeError(f"shard {shard_id} worker failed")
        if float(monitored["elapsed_seconds"]) > MAX_SINGLE_SHARD_SECONDS:
            raise RuntimeError(f"shard {shard_id} exceeded 30 minutes")
        if int(monitored["peak_rss_bytes"]) > MAX_RSS_BYTES:
            raise RuntimeError(f"shard {shard_id} exceeded the 12 GiB RSS budget")
        if _directory_size(recovery_root) > MAX_TEMP_BYTES:
            raise RuntimeError("recovery output exceeded the 50 GiB storage budget")
        checkpoint = verify_checkpoint(checkpoint_path)
        resource = {
            "format": "ip-002r-shard-resource-v1",
            "completed_at": _utc_now(),
            "raw_sample_sha256": sample["sha256"],
            "shard_count": 32,
            "shard_id": shard_id,
            "row_count": shard["row_count"],
            "elapsed_seconds": monitored["elapsed_seconds"],
            "peak_rss_bytes": monitored["peak_rss_bytes"],
            "peak_duckdb_temp_growth_bytes": monitored["peak_temp_growth_bytes"],
            "configured_duckdb_memory": DUCKDB_MEMORY_LIMIT,
            "configured_threads": DUCKDB_THREADS,
            "checkpoint_file_sha256": sha256_file(checkpoint_path),
            "checkpoint_payload_sha256": checkpoint["checkpoint_sha256"],
            "checkpoint_size_bytes": checkpoint_path.stat().st_size,
        }
        _atomic_create_json(resource_path, resource)
        resources.append(resource)
        print(
            json.dumps(
                {
                    "completed_shard": shard_id,
                    "row_count": shard["row_count"],
                    "elapsed_seconds": monitored["elapsed_seconds"],
                    "peak_rss_bytes": monitored["peak_rss_bytes"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
    return resources


def _all_checkpoint_resources(
    recovery_root: Path, pilot: Mapping[str, Any]
) -> list[dict[str, Any]]:
    resources = [
        {
            "shard_id": int(pilot["largest_shard_id"]),
            "elapsed_seconds": float(pilot["largest_shard_analysis_elapsed_seconds"]),
            "peak_rss_bytes": int(pilot["largest_shard_peak_rss_bytes"]),
            "peak_duckdb_temp_growth_bytes": int(
                pilot["largest_shard_duckdb_temp_peak_growth_bytes"]
            ),
            "checkpoint_size_bytes": int(pilot["checkpoint_size_bytes"]),
            "source": "feasibility-pilot",
        }
    ]
    for path in sorted((recovery_root / "resource-checkpoints-32").glob("shard-*.json")):
        value = _read_json(path)
        value["source"] = "remaining-shard-run"
        resources.append(value)
    ids = [int(item["shard_id"]) for item in resources]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate shard resource checkpoint")
    return sorted(resources, key=lambda item: int(item["shard_id"]))


def _reduce_august(
    *,
    recovery_root: Path,
    sample: Mapping[str, Any],
    manifest: Mapping[str, Any],
    pilot: Mapping[str, Any],
    repository: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    checkpoint_paths = sorted((recovery_root / "checkpoints-32").glob("shard-*.json"))
    if len(checkpoint_paths) != 32:
        raise RuntimeError(f"expected 32 shard checkpoints, found {len(checkpoint_paths)}")
    envelopes = [verify_checkpoint(path) for path in checkpoint_paths]
    forward = reduce_checkpoints(
        envelopes,
        expected_raw_sha256=str(sample["sha256"]),
        expected_shard_count=32,
    )
    reverse = reduce_checkpoints(
        list(reversed(envelopes)),
        expected_raw_sha256=str(sample["sha256"]),
        expected_shard_count=32,
    )
    forward_hash = hashlib.sha256(_canonical_json(forward)).hexdigest()
    reverse_hash = hashlib.sha256(_canonical_json(reverse)).hexdigest()
    if forward_hash != reverse_hash or forward != reverse:
        raise RuntimeError("August reducer depends on shard-processing order")
    recorded_attempt = sample["attempts"][-1]
    forward["provenance"].update(
        {
            "download_url": sample["download_url"],
            "local_path": sample["local_path"],
            "byte_length": sample["byte_length"],
            "sha256": sample["sha256"],
            "row_count": manifest["total_rows"],
            "row_group_count": recorded_attempt["row_group_count"],
            "download_attempts": sample["attempts"],
            "engine": "IP-002R memory-bounded recovery",
        }
    )
    code_shas = sorted(
        {str(envelope["payload"]["recovery_code_git_sha"]) for envelope in envelopes}
    )
    blob_shas = {sha: _recovery_blob_sha(repository, sha) for sha in code_shas}
    if len(set(blob_shas.values())) != 1:
        raise RuntimeError("checkpoint recovery analyzer module differs across code commits")
    resources = _all_checkpoint_resources(recovery_root, pilot)
    if {int(item["shard_id"]) for item in resources} != set(range(32)):
        raise RuntimeError("resource checkpoints are incomplete")
    checkpoint_integrity = [
        {
            "shard_id": int(envelope["payload"]["shard_id"]),
            "file_sha256": sha256_file(path),
            "payload_sha256": envelope["checkpoint_sha256"],
            "row_count": int(envelope["payload"]["row_count"]),
        }
        for path, envelope in zip(checkpoint_paths, envelopes, strict=True)
    ]
    metrics = {
        "status": "COMPLETE",
        "completed_at": _utc_now(),
        "raw_sample_sha256": sample["sha256"],
        "partition_elapsed_seconds": pilot["partition_elapsed_seconds"],
        "analysis_elapsed_seconds_sum": sum(
            float(item["elapsed_seconds"]) for item in resources
        ),
        "active_total_august_seconds": float(pilot["partition_elapsed_seconds"])
        + sum(float(item["elapsed_seconds"]) for item in resources),
        "peak_rss_bytes": max(int(item["peak_rss_bytes"]) for item in resources),
        "peak_duckdb_temp_growth_bytes": max(
            int(item["peak_duckdb_temp_growth_bytes"]) for item in resources
        ),
        "configured_duckdb_memory": DUCKDB_MEMORY_LIMIT,
        "configured_threads": DUCKDB_THREADS,
        "final_recovery_storage_bytes": _directory_size(recovery_root),
        "checkpoint_count": len(envelopes),
        "checkpoint_integrity": "PASS",
        "checkpoints": checkpoint_integrity,
        "recovery_code_git_shas": code_shas,
        "recovery_module_blob_shas": blob_shas,
        "reducer_forward_sha256": forward_hash,
        "reducer_reverse_sha256": reverse_hash,
        "reducer_order_independence": "PASS",
        "shard_resources": resources,
    }
    _immutable_json(recovery_root / "august-a1-a8.json", forward)
    _immutable_json(recovery_root / "august-recovery-summary.json", metrics)
    return forward, metrics


def _validation_fixture(
    june_sample: Mapping[str, Any], recovery_root: Path
) -> tuple[Path, Path, dict[str, Any]]:
    final_root = recovery_root / "june-equivalence"
    manifest_path = final_root / "validation-manifest.json"
    if manifest_path.is_file():
        manifest = _read_json(manifest_path)
        recovery_path = final_root / manifest["recovery_file"]
        reference_path = final_root / manifest["reference_file"]
        if sha256_file(recovery_path) != manifest["recovery_sha256"]:
            raise ValueError("June recovery-validation fixture hash mismatch")
        if sha256_file(reference_path) != manifest["reference_sha256"]:
            raise ValueError("June reference fixture hash mismatch")
        return recovery_path, reference_path, manifest
    if final_root.exists():
        raise FileExistsError(f"incomplete June validation directory exists: {final_root}")
    staging = recovery_root / f".june-equivalence.part-{os.getpid()}"
    staging.mkdir(parents=True)
    raw_path = Path(str(june_sample["local_path"])).resolve()
    expression = _validation_shard_sql()
    connection = duckdb.connect()
    started = time.perf_counter()
    try:
        connection.execute("SET memory_limit = '8GiB'")
        connection.execute("SET threads = 2")
        connection.execute("SET preserve_insertion_order = false")
        counts = connection.execute(
            f"""SELECT {expression} AS shard_id, COUNT(*)::BIGINT AS rows
                FROM read_parquet('{_sql_path(raw_path)}')
                GROUP BY shard_id ORDER BY shard_id"""
        ).fetchall()
        selected_id, selected_rows = min(
            ((int(row[0]), int(row[1])) for row in counts if int(row[1]) > 0),
            key=lambda item: item[0],
        )
        recovery_path = staging / "recovery.parquet"
        reference_path = staging / "reference.parquet"
        connection.execute(
            f"""COPY (
                    SELECT * EXCLUDE (file_row_number),
                           file_row_number::UBIGINT AS {SOURCE_ROW_ORDINAL}
                    FROM read_parquet('{_sql_path(raw_path)}', file_row_number=true)
                    WHERE {expression} = {selected_id}
                ) TO '{_sql_path(recovery_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
        connection.execute(
            f"""COPY (
                    SELECT * EXCLUDE ({SOURCE_ROW_ORDINAL})
                    FROM read_parquet('{_sql_path(recovery_path)}', hive_partitioning=false)
                    ORDER BY {SOURCE_ROW_ORDINAL}
                ) TO '{_sql_path(reference_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"""
        )
    finally:
        connection.close()
    elapsed = time.perf_counter() - started
    manifest = {
        "format": "ip-002r-june-equivalence-fixture-v1",
        "created_at": _utc_now(),
        "raw_june_sha256": june_sample["sha256"],
        "selection": "lowest non-empty deterministic (market, asset_id) shard",
        "shard_algorithm": SHARD_ALGORITHM,
        "shard_count": JUNE_VALIDATION_SHARDS,
        "selected_shard_id": selected_id,
        "row_count": selected_rows,
        "elapsed_seconds": elapsed,
        "recovery_file": recovery_path.name,
        "recovery_sha256": sha256_file(recovery_path),
        "reference_file": reference_path.name,
        "reference_sha256": sha256_file(reference_path),
    }
    _atomic_create_json(staging / "validation-manifest.json", manifest)
    os.replace(staging, final_root)
    return final_root / "recovery.parquet", final_root / "reference.parquet", manifest


def _validation_sample(
    june_sample: Mapping[str, Any], path: Path, outcome: str
) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        rows = int(
            connection.execute(
                "SELECT num_rows FROM parquet_file_metadata(?)", [str(path)]
            ).fetchone()[0]
        )
    finally:
        connection.close()
    return {
        **june_sample,
        "local_path": str(path),
        "sha256": sha256_file(path),
        "attempts": [{"outcome": outcome, "row_count": rows}],
    }


def validate_june_equivalence(
    *, original_output: Path, recovery_root: Path
) -> dict[str, Any]:
    result_path = recovery_root / "june-equivalence-result.json"
    if result_path.is_file():
        result = _read_json(result_path)
        if result.get("status") != "PASS":
            raise RuntimeError("preserved June equivalence validation did not pass")
        return result
    evidence = verify_preserved_evidence(original_output)
    june = next(
        item for item in evidence["samples"] if item["requested_hour"] == "2026-06-15T12"
    )
    recovery_path, reference_path, manifest = _validation_fixture(june, recovery_root)
    reference = analyze_file(
        _validation_sample(june, reference_path, "IP_002R_REFERENCE_FIXTURE"),
        working_directory=recovery_root / "june-equivalence-work" / "reference",
        source_schema_path=Path(str(june["local_path"])),
    )
    recovered = analyze_file(
        _validation_sample(june, recovery_path, "IP_002R_RECOVERY_FIXTURE"),
        working_directory=recovery_root / "june-equivalence-work" / "recovery",
        source_schema_path=Path(str(june["local_path"])),
        source_row_ordinal_column=SOURCE_ROW_ORDINAL,
        duckdb_memory_limit=DUCKDB_MEMORY_LIMIT,
        duckdb_threads=DUCKDB_THREADS,
    )
    scientific_reference = {key: reference[key] for key in [f"a{i}" for i in range(1, 9)]}
    scientific_recovered = {key: recovered[key] for key in [f"a{i}" for i in range(1, 9)]}
    reference_hash = hashlib.sha256(_canonical_json(scientific_reference)).hexdigest()
    recovery_hash = hashlib.sha256(_canonical_json(scientific_recovered)).hexdigest()
    if scientific_reference != scientific_recovered or reference_hash != recovery_hash:
        raise RuntimeError("June reference and recovery engines differ on validation unit")
    result = {
        "status": "PASS",
        "completed_at": _utc_now(),
        "raw_june_sha256": june["sha256"],
        "original_full_june_rerun": False,
        "validation_manifest": manifest,
        "reference_a1_a8_sha256": reference_hash,
        "recovery_a1_a8_sha256": recovery_hash,
        "exact_a1_a8_equality": True,
        "source_row_ordinal_used_only_for_a6_diagnostic": True,
    }
    return _immutable_json(result_path, result)


def complete_recovery(
    *,
    original_output: Path,
    recovery_root: Path,
    repository: Path,
    offline_test_result: str,
) -> dict[str, Any]:
    """Reject the obsolete completion path before any shard can be launched."""

    raise IP002RClosedError(IP_002R_CLOSED_MESSAGE)


def finalize_recovery(
    *,
    recovery_root: Path,
    repository: Path,
    report_path: Path,
    feasibility_label: str,
    feasibility_rationale: str,
    final_test_result: str,
) -> dict[str, Any]:
    """Reject obsolete A9/report finalization for the closed recovery attempt."""

    raise IP002RClosedError(IP_002R_CLOSED_MESSAGE)
