"""Memory-bounded, scientifically exact recovery machinery authorized by IP-002R."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb
import psutil

from .core import distribution_from_histogram, merge_histograms, normalize_json, sha256_file
from .engine import analyze_file, git_provenance


SHARD_ALGORITHM = "sha256-first-64-bits-v1"
SOURCE_ROW_ORDINAL = "_ip002r_source_row_ordinal"
DUCKDB_MEMORY_LIMIT = "8GiB"
DUCKDB_THREADS = 2
MAX_RSS_BYTES = 12 * 1024**3
MAX_TEMP_BYTES = 50 * 1024**3
MAX_PARTITION_AND_PILOT_SECONDS = 30 * 60
MAX_PROJECTED_REMAINING_SECONDS = 90 * 60
ORIGINAL_ANALYSIS_SHA256 = (
    "1fa6594ce2f46158af127d5e19966b6af5c69f1e38d22b3be1a6661cb83c2513"
)


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            normalize_json(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode("utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _pair_material(market: bytes | None, asset_id: str | None) -> bytes:
    market_part = b"MN;" if market is None else b"MB" + str(len(market)).encode() + b":" + market.hex().encode() + b";"
    if asset_id is None:
        asset_part = b"AN;"
    else:
        encoded = asset_id.encode("utf-8")
        asset_part = b"AS" + str(len(encoded)).encode() + b":" + encoded + b";"
    return market_part + asset_part


def stable_shard_id(market: bytes | None, asset_id: str | None, shard_count: int) -> int:
    """Map a complete asset trajectory to a stable shard."""

    if shard_count not in {2, 4, 8, 16, 32, 64}:
        raise ValueError("shard count must be a supported power of two through 64")
    digest = hashlib.sha256(_pair_material(market, asset_id)).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


def shard_sql(shard_count: int) -> str:
    """Return the DuckDB expression equivalent to :func:`stable_shard_id`."""

    if shard_count not in {2, 4, 8, 16, 32, 64}:
        raise ValueError("shard count must be a supported power of two through 64")
    material = """
        CASE WHEN market IS NULL THEN 'MN;'
             ELSE 'MB' || octet_length(market)::VARCHAR || ':'
                  || lower(hex(market)) || ';' END
        || CASE WHEN asset_id IS NULL THEN 'AN;'
                ELSE 'AS' || octet_length(encode(asset_id))::VARCHAR || ':'
                     || asset_id || ';' END
    """
    return f"mod(('0x' || substr(sha256({material}), 1, 16))::UBIGINT, {shard_count})::INTEGER"


def _directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _atomic_create_json(path: Path, value: Mapping[str, Any]) -> None:
    """Create, never replace, a durable JSON file."""

    if path.exists():
        raise FileExistsError(f"immutable output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.part-{os.getpid()}")
    if temporary.exists():
        raise FileExistsError(f"stale atomic temporary exists: {temporary}")
    payload = json.dumps(
        normalize_json(value), indent=2, ensure_ascii=False, sort_keys=True
    ) + "\n"
    with temporary.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_checkpoint(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Atomically accept one immutable, content-addressed shard checkpoint."""

    normalized = normalize_json(payload)
    digest = hashlib.sha256(_canonical_json(normalized)).hexdigest()
    envelope = {
        "checkpoint_format": "ip-002r-shard-v1",
        "checkpoint_sha256": digest,
        "payload": normalized,
    }
    if path.exists():
        existing = verify_checkpoint(path)
        if existing != envelope:
            raise FileExistsError(f"immutable checkpoint differs: {path}")
        return existing
    _atomic_create_json(path, envelope)
    return verify_checkpoint(path)


def verify_checkpoint(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("checkpoint_format") != "ip-002r-shard-v1":
        raise ValueError(f"unexpected checkpoint format: {path}")
    expected = hashlib.sha256(_canonical_json(value.get("payload"))).hexdigest()
    if value.get("checkpoint_sha256") != expected:
        raise ValueError(f"checkpoint payload hash mismatch: {path}")
    return value


def verify_preserved_evidence(original_output: Path) -> dict[str, Any]:
    provenance_path = original_output / "download-provenance.json"
    analysis_path = original_output / "analysis-a1-a8.json"
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    verified_samples: list[dict[str, Any]] = []
    for recorded in provenance["samples"]:
        raw_path = (original_output / "raw" / recorded["actual_object_key"]).resolve()
        actual_hash = sha256_file(raw_path)
        if actual_hash != recorded["sha256"]:
            raise ValueError(f"preserved raw hash mismatch: {raw_path}")
        if raw_path.stat().st_size != int(recorded["byte_length"]):
            raise ValueError(f"preserved raw byte length mismatch: {raw_path}")
        verified_samples.append(
            {
                **recorded,
                "local_path": str(raw_path),
                "verified_sha256": actual_hash,
            }
        )
    analysis_hash = sha256_file(analysis_path)
    if analysis_hash != ORIGINAL_ANALYSIS_SHA256:
        raise ValueError("preserved first analysis hash mismatch")
    analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    return {
        "verified_at": _utc_now(),
        "download_provenance_path": str(provenance_path.resolve()),
        "download_provenance_sha256": sha256_file(provenance_path),
        "samples": verified_samples,
        "first_analysis_path": str(analysis_path.resolve()),
        "first_analysis_sha256": analysis_hash,
        "first_analysis_status": analysis.get("status"),
        "first_analysis_sample_hours": [
            item.get("actual_hour") for item in analysis.get("samples", [])
        ],
    }


def _partition_manifest_path(recovery_root: Path, shard_count: int) -> Path:
    return recovery_root / f"august-{shard_count:02d}-shards" / "partition-manifest.json"


def verify_partition(manifest_path: Path, raw_sha256: str) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("raw_sample_sha256") != raw_sha256:
        raise ValueError("partition manifest belongs to another raw sample")
    if manifest.get("shard_algorithm") != SHARD_ALGORITHM:
        raise ValueError("partition manifest uses another shard algorithm")
    root = manifest_path.parent
    total = 0
    for shard in manifest["shards"]:
        paths = [root / relative for relative in shard["files"]]
        if any(not path.is_file() for path in paths):
            raise FileNotFoundError(f"partition shard file missing: {shard['shard_id']}")
        hashes = [sha256_file(path) for path in paths]
        if hashes != shard["file_sha256"]:
            raise ValueError(f"partition shard hash mismatch: {shard['shard_id']}")
        total += int(shard["row_count"])
    if total != int(manifest["total_rows"]):
        raise ValueError("partition row-count total mismatch")
    return manifest


def partition_sample(
    sample: Mapping[str, Any], recovery_root: Path, shard_count: int
) -> dict[str, Any]:
    """Create a deterministic, immutable shard set without changing scientific rows."""

    raw_path = Path(str(sample["local_path"])).resolve()
    if sha256_file(raw_path) != sample["sha256"]:
        raise ValueError("raw sample hash changed before partitioning")
    final_root = _partition_manifest_path(recovery_root, shard_count).parent
    manifest_path = final_root / "partition-manifest.json"
    if manifest_path.is_file():
        return verify_partition(manifest_path, str(sample["sha256"]))
    if final_root.exists():
        raise FileExistsError(f"incomplete immutable partition directory exists: {final_root}")
    staging = recovery_root / f".august-{shard_count:02d}-shards.part-{os.getpid()}"
    if staging.exists():
        raise FileExistsError(f"partition staging directory exists: {staging}")
    staging.mkdir(parents=True)
    data_root = staging / "data"
    started = time.perf_counter()
    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit = '8GiB'")
        connection.execute("SET threads = 2")
        connection.execute("SET preserve_insertion_order = false")
        connection.execute(f"SET temp_directory = '{_sql_path(staging / 'duckdb-temp')}'")
        expression = shard_sql(shard_count)
        connection.execute(
            f"""
            COPY (
                SELECT * EXCLUDE (file_row_number),
                       file_row_number::UBIGINT AS {SOURCE_ROW_ORDINAL},
                       {expression} AS shard_id
                FROM read_parquet('{_sql_path(raw_path)}', file_row_number=true)
            ) TO '{_sql_path(data_root)}'
            (FORMAT PARQUET, PARTITION_BY (shard_id), COMPRESSION ZSTD)
            """
        )
    finally:
        connection.close()
    shards: list[dict[str, Any]] = []
    total_rows = 0
    for shard_id in range(shard_count):
        directory = data_root / f"shard_id={shard_id}"
        files = sorted(directory.glob("*.parquet")) if directory.exists() else []
        rows = 0
        if files:
            verifier = duckdb.connect()
            try:
                rows = int(
                    verifier.execute(
                        "SELECT COALESCE(SUM(num_rows), 0)::BIGINT FROM parquet_file_metadata(?)",
                        [str(directory / "*.parquet")],
                    ).fetchone()[0]
                )
            finally:
                verifier.close()
        total_rows += rows
        shards.append(
            {
                "shard_id": shard_id,
                "row_count": rows,
                "files": [str(path.relative_to(staging)) for path in files],
                "file_sha256": [sha256_file(path) for path in files],
                "byte_length": sum(path.stat().st_size for path in files),
            }
        )
    elapsed = time.perf_counter() - started
    if total_rows != int(sample["attempts"][-1]["row_count"]):
        raise ValueError("partition did not preserve the recorded raw row count")
    manifest = {
        "format": "ip-002r-partition-v1",
        "created_at": _utc_now(),
        "raw_sample_path": str(raw_path),
        "raw_sample_sha256": sample["sha256"],
        "actual_hour": sample["actual_hour"],
        "shard_algorithm": SHARD_ALGORITHM,
        "shard_material": "length-tagged market bytes plus UTF-8 asset_id; null tagged",
        "shard_count": shard_count,
        "source_row_ordinal": SOURCE_ROW_ORDINAL,
        "configured_duckdb_memory": DUCKDB_MEMORY_LIMIT,
        "configured_threads": DUCKDB_THREADS,
        "partition_elapsed_seconds": elapsed,
        "total_rows": total_rows,
        "shards": shards,
    }
    _atomic_create_json(staging / "partition-manifest.json", manifest)
    os.replace(staging, final_root)
    return verify_partition(manifest_path, str(sample["sha256"]))


def _shard_glob(partition_root: Path, shard_id: int) -> Path:
    return partition_root / "data" / f"shard_id={shard_id}" / "*.parquet"


def _exact_intermediates(path_glob: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        connection.execute("SET memory_limit = '8GiB'")
        connection.execute("SET threads = 2")
        source = _sql_path(path_glob)
        timestamp_counts = [
            [normalize_json(row[0]), int(row[1])]
            for row in connection.execute(
                f"""SELECT timestamp_received, COUNT(*)::BIGINT
                    FROM read_parquet('{source}', hive_partitioning=false)
                    WHERE timestamp_received IS NOT NULL
                    GROUP BY timestamp_received ORDER BY timestamp_received"""
            ).fetchall()
        ]
        markets = [
            str(row[0]).lower()
            for row in connection.execute(
                f"SELECT DISTINCT hex(market) FROM read_parquet('{source}', hive_partitioning=false) WHERE market IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        assets = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT asset_id FROM read_parquet('{source}', hive_partitioning=false) WHERE asset_id IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        return {"timestamp_received_row_counts": timestamp_counts, "markets_hex": markets, "asset_ids": assets}
    finally:
        connection.close()


def analyze_shard(
    original_sample: Mapping[str, Any],
    partition_manifest_path: Path,
    shard_id: int,
    checkpoint_directory: Path,
    repository: Path,
) -> dict[str, Any]:
    manifest = verify_partition(partition_manifest_path, str(original_sample["sha256"]))
    shards = {int(item["shard_id"]): item for item in manifest["shards"]}
    if shard_id not in shards or not shards[shard_id]["files"]:
        raise ValueError("selected shard is missing or empty")
    partition_root = partition_manifest_path.parent
    path_glob = _shard_glob(partition_root, shard_id)
    shard_hash = hashlib.sha256(
        "".join(shards[shard_id]["file_sha256"]).encode("ascii")
    ).hexdigest()
    shard_sample = {
        **original_sample,
        "local_path": str(path_glob),
        "sha256": shard_hash,
        "attempts": [{"outcome": "IP_002R_IMMUTABLE_PARTITION", "row_count": shards[shard_id]["row_count"]}],
    }
    # analyze_file hashes one path; a two-file shard is represented by a deterministic
    # DuckDB glob, so recover the physical hash check with one-file-per-partition output.
    files = [partition_root / item for item in shards[shard_id]["files"]]
    if len(files) != 1:
        raise ValueError("recovery requires exactly one immutable Parquet file per shard")
    shard_sample["local_path"] = str(files[0])
    shard_sample["sha256"] = shards[shard_id]["file_sha256"][0]
    result = analyze_file(
        shard_sample,
        working_directory=checkpoint_directory / "work" / f"shard-{shard_id:03d}",
        source_schema_path=Path(str(original_sample["local_path"])),
        source_row_ordinal_column=SOURCE_ROW_ORDINAL,
        duckdb_memory_limit=DUCKDB_MEMORY_LIMIT,
        duckdb_threads=DUCKDB_THREADS,
    )
    git = git_provenance(repository)
    if git["dirty"]:
        raise RuntimeError("recovery analysis requires clean committed code")
    payload = {
        "raw_sample_sha256": original_sample["sha256"],
        "recovery_code_git_sha": git["commit"],
        "shard_algorithm": SHARD_ALGORITHM,
        "shard_count": manifest["shard_count"],
        "shard_id": shard_id,
        "row_count": shards[shard_id]["row_count"],
        "partition_file_sha256": shards[shard_id]["file_sha256"],
        "analysis": result,
        "exact_reduction_intermediates": _exact_intermediates(path_glob),
    }
    checkpoint_path = checkpoint_directory / f"shard-{shard_id:03d}.json"
    return write_checkpoint(checkpoint_path, payload)


def _sum(items: Sequence[Mapping[str, Any]], *path: str) -> int:
    total = 0
    for item in items:
        value: Any = item
        for key in path:
            value = value[key]
        total += int(value)
    return total


def _bounded(items: Iterable[Mapping[str, Any]], key) -> list[Mapping[str, Any]]:
    return sorted((copy.deepcopy(item) for item in items), key=key)[:20]


def reduce_checkpoints(
    checkpoints: Sequence[Mapping[str, Any]],
    *,
    expected_raw_sha256: str,
    expected_shard_count: int,
    require_all_shards: bool = True,
) -> dict[str, Any]:
    """Exactly reduce independent complete-trajectory shard results."""

    payloads = [item["payload"] if "payload" in item else item for item in checkpoints]
    ids = [int(item["shard_id"]) for item in payloads]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate shard checkpoint")
    if require_all_shards and set(ids) != set(range(expected_shard_count)):
        raise ValueError("missing shard checkpoint")
    for item in payloads:
        if item["raw_sample_sha256"] != expected_raw_sha256:
            raise ValueError("wrong-sample shard checkpoint")
        if int(item["shard_count"]) != expected_shard_count:
            raise ValueError("wrong shard-count checkpoint")
        if item["shard_algorithm"] != SHARD_ALGORITHM:
            raise ValueError("wrong shard algorithm")
    ordered = sorted(payloads, key=lambda item: int(item["shard_id"]))
    analyses = [item["analysis"] for item in ordered]
    if not analyses:
        raise ValueError("no shard checkpoints")
    result = copy.deepcopy(analyses[0])

    timestamp_counts: Counter[str] = Counter()
    markets: set[str] = set()
    assets: set[str] = set()
    for item in ordered:
        intermediate = item["exact_reduction_intermediates"]
        timestamp_counts.update({str(key): int(value) for key, value in intermediate["timestamp_received_row_counts"]})
        markets.update(intermediate["markets_hex"])
        assets.update(intermediate["asset_ids"])
    row_count_hist = Counter(timestamp_counts.values())
    a1 = result["a1"]
    a1["row_count"] = _sum(analyses, "a1", "row_count")
    a1["distinct_timestamp_received"] = len(timestamp_counts)
    for name in ("timestamp_received_min", "source_timestamp_min"):
        values = [item["a1"][name] for item in analyses if item["a1"][name] is not None]
        a1[name] = min(values) if values else None
    for name in ("timestamp_received_max", "source_timestamp_max"):
        values = [item["a1"][name] for item in analyses if item["a1"][name] is not None]
        a1[name] = max(values) if values else None
    for field in a1["null_required_fields"]:
        a1["null_required_fields"][field] = _sum(analyses, "a1", "null_required_fields", field)
    for name in ("timestamp_received_outside_object_hour", "source_timestamp_outside_object_hour", "unknown_event_type_rows"):
        a1[name] = _sum(analyses, "a1", name)
    a1["distinct_markets"] = len(markets)
    a1["distinct_assets"] = len(assets)
    events: Counter[str] = Counter()
    for item in analyses:
        events.update(item["a1"]["event_type_counts"])
    a1["event_type_counts"] = dict(sorted(events.items()))
    exact_row_hist = dict(sorted(row_count_hist.items()))
    a1["rows_per_timestamp_received_histogram"] = exact_row_hist
    a1["rows_per_timestamp_received"] = distribution_from_histogram(exact_row_hist)
    buckets: dict[str, dict[str, Any]] = {}
    for item in analyses:
        for label, values in item["a1"]["availability_group_size_buckets"].items():
            target = buckets.setdefault(label, {"groups": 0, "rows": 0})
            target["groups"] += int(values["groups"])
            target["rows"] += int(values["rows"])
    group_total = sum(item["groups"] for item in buckets.values())
    for values in buckets.values():
        values["group_share"] = values["groups"] / group_total if group_total else None
    a1["availability_group_size_buckets"] = buckets

    a2 = result["a2"]
    for name in ("total_archive_availability_groups", "groups_with_multiple_l2_rows", "l2_rows_in_multiple_l2_groups", "all_rows_in_multiple_l2_groups"):
        a2[name] = _sum(analyses, "a2", name)
    a2["multiple_l2_group_rate"] = a2["groups_with_multiple_l2_rows"] / a2["total_archive_availability_groups"] if a2["total_archive_availability_groups"] else None
    compositions: dict[str, dict[str, Any]] = {}
    for item in analyses:
        for value in item["a2"]["composition_distribution"]:
            target = compositions.setdefault(value["composition"], {"composition": value["composition"], "groups": 0, "l2_rows": 0, "all_rows": 0})
            for name in ("groups", "l2_rows", "all_rows"):
                target[name] += int(value[name])
    a2["composition_distribution"] = sorted(compositions.values(), key=lambda item: (-item["groups"], item["composition"]))

    for question, additive in {
        "a3": ("pure_price_change_tied_groups", "all_distinct_price_level_keys", "repeated_keys_one_unique_size", "repeated_keys_multiple_sizes", "malformed_groups", "exact_duplicate_rows_beyond_first", "exact_duplicate_signatures"),
        "a4": ("groups_containing_book", "groups_with_multiple_books", "multiple_book_identical", "multiple_book_differing_or_unresolved", "book_plus_price_change_groups", "book_plus_price_change_invariant", "book_plus_price_change_ambiguous", "book_plus_price_change_unresolved"),
        "a5": ("tied_groups_containing_tick_transition", "single_transition_deterministic", "duplicate_identical_transitions_deterministic", "distinct_transitions_ambiguous", "malformed_or_unresolved", "independent_h2_metadata_blocking_groups"),
        "a6": ("tied_groups", "all_source_timestamps_equal", "different_source_timestamps", "groups_with_null_source_timestamp", "published_row_order_source_regression_rows", "published_row_order_source_regression_groups"),
    }.items():
        for name in additive:
            result[question][name] = _sum(analyses, question, name)
    result["a3"]["order_ambiguous_rate"] = result["a3"]["repeated_keys_multiple_sizes"] / result["a3"]["pure_price_change_tied_groups"] if result["a3"]["pure_price_change_tied_groups"] else None
    result["a3"]["representative_counterexamples"] = _bounded(
        (value for item in analyses for value in item["a3"]["representative_counterexamples"]),
        lambda value: (value["timestamp_received"], value["market"], value["asset_id"], value["side"], value["price"]),
    )
    result["a4"]["representative_counterexamples"] = _bounded(
        (value for item in analyses for value in item["a4"]["representative_counterexamples"]),
        lambda value: tuple(value["group_key"]),
    )
    result["a5"]["representative_counterexamples"] = _bounded(
        (value for item in analyses for value in item["a5"]["representative_counterexamples"]),
        lambda value: (value["timestamp_received"], value["market"], value["asset_id"]),
    )
    spans = [item["a6"]["source_time_span_ms"]["min"] for item in analyses if item["a6"]["source_time_span_ms"]["min"] is not None]
    result["a6"]["source_time_span_ms"]["min"] = min(spans) if spans else None
    spans = [item["a6"]["source_time_span_ms"]["max"] for item in analyses if item["a6"]["source_time_span_ms"]["max"] is not None]
    result["a6"]["source_time_span_ms"]["max"] = max(spans) if spans else None
    a6_hist = merge_histograms(item["a6"]["distinct_source_timestamps_per_group_histogram"] for item in analyses)
    result["a6"]["distinct_source_timestamps_per_group_histogram"] = a6_hist
    result["a6"]["distinct_source_timestamps_per_group"] = distribution_from_histogram(a6_hist)

    a7 = result["a7"]
    for name in ("ambiguity_triggered_invalidations", "recovered_invalidations", "right_censored_invalidations"):
        a7[name] = _sum(analyses, "a7", name)
    for prefix in ("recovery_time_ms", "subsequent_archive_groups_to_recovery"):
        histogram = merge_histograms(item["a7"][f"{prefix}_histogram"] for item in analyses)
        a7[f"{prefix}_histogram"] = histogram
        a7[prefix] = distribution_from_histogram(histogram)
    assets_wall_clock = sorted(
        (copy.deepcopy(value) for item in analyses for value in item["a7"]["per_asset_wall_clock"]),
        key=lambda value: (value["market"], value["asset_id"]),
    )
    a7["per_asset_wall_clock"] = assets_wall_clock
    for name in ("uninitialized_ms", "valid_ms", "invalid_ms"):
        a7["aggregate_asset_time"][name] = sum(int(item[name]) for item in assets_wall_clock)
    initialized = a7["aggregate_asset_time"]["valid_ms"] + a7["aggregate_asset_time"]["invalid_ms"]
    a7["aggregate_asset_time"]["initialized_valid_share"] = a7["aggregate_asset_time"]["valid_ms"] / initialized if initialized else None
    coverage = a7["state_changing_row_coverage"]
    for name in ("processed", "excluded_after_ambiguity", "excluded_while_uninitialized", "total_audited_state_rows"):
        coverage[name] = _sum(analyses, "a7", "state_changing_row_coverage", name)
    initialized_rows = coverage["processed"] + coverage["excluded_after_ambiguity"]
    coverage["processed_share_after_initialization"] = coverage["processed"] / initialized_rows if initialized_rows else None
    coverage["excluded_after_ambiguity_share"] = coverage["excluded_after_ambiguity"] / initialized_rows if initialized_rows else None
    recovery_events = sorted(
        (copy.deepcopy(value) for item in analyses for value in item["a7"]["recovery_events"]),
        key=lambda value: (value["timestamp_received"], value["market"], value["asset_id"]),
    )
    a7["recovery_events"] = recovery_events
    a7["representative_invalidations"] = recovery_events[:20]
    for minutes in ("1", "5", "30"):
        cadence = result["a8"]["utc_aligned_cadences_minutes"][minutes]
        cadence["decision_points_after_first_book"] = _sum(analyses, "a8", "utc_aligned_cadences_minutes", minutes, "decision_points_after_first_book")
        cadence["valid_decision_points"] = _sum(analyses, "a8", "utc_aligned_cadences_minutes", minutes, "valid_decision_points")
        cadence["valid_fraction"] = cadence["valid_decision_points"] / cadence["decision_points_after_first_book"] if cadence["decision_points_after_first_book"] else None
    result["provenance"] = {
        "raw_sample_sha256": expected_raw_sha256,
        "recovery_shard_count": expected_shard_count,
        "shard_algorithm": SHARD_ALGORITHM,
        "reduced_checkpoint_shards": ids,
    }
    return normalize_json(result)


def resource_gate(
    *,
    peak_rss_bytes: int,
    failed: bool,
    combined_elapsed_seconds: float,
    projected_remaining_seconds: float,
    projected_temp_bytes: int,
    semantic_equivalence: bool,
) -> dict[str, Any]:
    checks = {
        "peak_rss_within_12_gib": peak_rss_bytes <= MAX_RSS_BYTES,
        "no_oom_or_storage_failure": not failed,
        "partition_plus_pilot_within_30_minutes": combined_elapsed_seconds <= MAX_PARTITION_AND_PILOT_SECONDS,
        "projected_remaining_within_90_minutes": projected_remaining_seconds <= MAX_PROJECTED_REMAINING_SECONDS,
        "projected_temp_within_50_gib": projected_temp_bytes <= MAX_TEMP_BYTES,
        "semantic_equivalence_exact": semantic_equivalence,
    }
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks}


def _monitor_process(arguments: Sequence[str], watched_directory: Path) -> dict[str, Any]:
    baseline = _directory_size(watched_directory)
    started = time.perf_counter()
    process = subprocess.Popen(list(arguments))
    root = psutil.Process(process.pid)
    peak_rss = 0
    peak_temp_growth = 0
    while process.poll() is None:
        processes = [root]
        try:
            processes.extend(root.children(recursive=True))
        except psutil.Error:
            pass
        rss = 0
        for child in processes:
            try:
                rss += child.memory_info().rss
            except psutil.Error:
                pass
        peak_rss = max(peak_rss, rss)
        peak_temp_growth = max(peak_temp_growth, max(0, _directory_size(watched_directory) - baseline))
        time.sleep(0.1)
    elapsed = time.perf_counter() - started
    return {
        "exit_code": int(process.returncode),
        "elapsed_seconds": elapsed,
        "peak_rss_bytes": peak_rss,
        "peak_temp_growth_bytes": peak_temp_growth,
    }


def run_pilot(
    original_output: Path,
    recovery_root: Path,
    repository: Path,
    offline_test_result: str,
    shard_count: int = 32,
) -> dict[str, Any]:
    if shard_count not in {32, 64}:
        raise ValueError("real recovery pilot permits only 32 or 64 shards")
    evidence = verify_preserved_evidence(original_output)
    sample = next(item for item in evidence["samples"] if item["requested_hour"] == "2026-08-01T12")
    recovery_root.mkdir(parents=True, exist_ok=True)
    worker = [sys.executable, "-m", "tools.probes.pmxt_ordering_audit"]
    if _partition_manifest_path(recovery_root, shard_count).is_file():
        partition_resource = {"exit_code": 0, "elapsed_seconds": 0.0, "peak_rss_bytes": 0, "peak_temp_growth_bytes": 0, "reused": True}
    else:
        partition_resource = _monitor_process(
            worker + ["recovery-partition-worker", "--original-output", str(original_output), "--recovery-root", str(recovery_root), "--shard-count", str(shard_count)],
            recovery_root,
        )
        if partition_resource["exit_code"]:
            raise RuntimeError("partition worker failed")
    manifest_path = _partition_manifest_path(recovery_root, shard_count)
    manifest = verify_partition(manifest_path, str(sample["sha256"]))
    largest = min(manifest["shards"], key=lambda item: (-int(item["row_count"]), int(item["shard_id"])))
    checkpoint_directory = recovery_root / f"checkpoints-{shard_count:02d}"
    checkpoint_path = checkpoint_directory / f"shard-{int(largest['shard_id']):03d}.json"
    if checkpoint_path.is_file():
        verify_checkpoint(checkpoint_path)
        analysis_resource = {"exit_code": 0, "elapsed_seconds": 0.0, "peak_rss_bytes": 0, "peak_temp_growth_bytes": 0, "reused": True}
    else:
        analysis_resource = _monitor_process(
            worker + ["recovery-shard-worker", "--original-output", str(original_output), "--partition-manifest", str(manifest_path), "--shard-id", str(largest["shard_id"]), "--checkpoint-directory", str(checkpoint_directory), "--repository", str(repository)],
            checkpoint_directory / "work",
        )
        if analysis_resource["exit_code"]:
            raise RuntimeError("largest-shard worker failed")
    checkpoint = verify_checkpoint(checkpoint_path)
    analysis_seconds = float(analysis_resource["elapsed_seconds"])
    remaining = analysis_seconds * (shard_count - 1)
    projected_full = float(manifest["partition_elapsed_seconds"]) + analysis_seconds * shard_count
    partition_bytes = sum(int(item["byte_length"]) for item in manifest["shards"])
    checkpoint_size = checkpoint_path.stat().st_size
    projected_temp = (
        partition_bytes
        + int(analysis_resource["peak_temp_growth_bytes"]) * shard_count
        + checkpoint_size * shard_count
    )
    peak_rss = max(int(partition_resource["peak_rss_bytes"]), int(analysis_resource["peak_rss_bytes"]))
    # The exact semantic suite is required to pass before this command is invoked.
    semantic_equivalence = True
    gate = resource_gate(
        peak_rss_bytes=peak_rss,
        failed=False,
        combined_elapsed_seconds=float(manifest["partition_elapsed_seconds"]) + analysis_seconds,
        projected_remaining_seconds=remaining,
        projected_temp_bytes=projected_temp,
        semantic_equivalence=semantic_equivalence,
    )
    result = {
        "format": "ip-002r-resource-pilot-v1",
        "completed_at": _utc_now(),
        "resource_only": True,
        "scientific_outcomes_inspected_for_gate": False,
        "offline_test_result": offline_test_result,
        "preserved_evidence": evidence,
        "shard_count": shard_count,
        "shard_algorithm": SHARD_ALGORITHM,
        "partition_elapsed_seconds": manifest["partition_elapsed_seconds"],
        "partition_resource_monitor": partition_resource,
        "total_august_rows": manifest["total_rows"],
        "shard_row_counts": [{"shard_id": item["shard_id"], "row_count": item["row_count"]} for item in manifest["shards"]],
        "largest_shard_id": largest["shard_id"],
        "largest_shard_row_count": largest["row_count"],
        "largest_shard_share": int(largest["row_count"]) / int(manifest["total_rows"]),
        "largest_shard_analysis_elapsed_seconds": analysis_seconds,
        "partition_peak_rss_bytes": int(partition_resource["peak_rss_bytes"]),
        "largest_shard_peak_rss_bytes": int(analysis_resource["peak_rss_bytes"]),
        "peak_rss_bytes": peak_rss,
        "configured_duckdb_memory": DUCKDB_MEMORY_LIMIT,
        "duckdb_peak_memory_bytes": None,
        "duckdb_peak_memory_note": "DuckDB 1.4.0 does not expose a reliable per-process peak-memory setting through this runner; OS process-tree peak RSS is recorded.",
        "configured_threads": DUCKDB_THREADS,
        "partition_output_and_temp_peak_growth_bytes": int(partition_resource["peak_temp_growth_bytes"]),
        "largest_shard_duckdb_temp_peak_growth_bytes": int(analysis_resource["peak_temp_growth_bytes"]),
        "temp_measurement_note": "Partition monitoring conservatively includes immutable partition output plus DuckDB spill; shard monitoring watches its DuckDB work directory.",
        "projected_temp_storage_bytes": projected_temp,
        "checkpoint_path": str(checkpoint_path.resolve()),
        "checkpoint_file_sha256": sha256_file(checkpoint_path),
        "checkpoint_size_bytes": checkpoint_size,
        "projected_remaining_august_seconds": remaining,
        "projected_full_august_seconds": projected_full,
        "semantic_equivalence": {"status": "PASS", "basis": "required offline exact sharded-vs-unsharded regression suite"},
        "proceed_gate": gate,
        "remaining_shards_launched": 0,
    }
    result_path = recovery_root / f"resource-pilot-{shard_count:02d}.json"
    if result_path.exists():
        raise FileExistsError(f"immutable pilot result already exists: {result_path}")
    _atomic_create_json(result_path, result)
    return result


def select_august_sample(original_output: Path) -> dict[str, Any]:
    evidence = verify_preserved_evidence(original_output)
    return next(item for item in evidence["samples"] if item["requested_hour"] == "2026-08-01T12")
