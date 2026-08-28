"""Public, bounded pmxt sample acquisition for IP-002."""

from __future__ import annotations

import http.client
import hashlib
import json
import os
import ssl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb

from .core import PREDECLARED_SAMPLE_HOURS, candidate_hours, sha256_file


ARCHIVE_HOST = "r2v2.pmxt.dev"
ARCHIVE_PREFIX = "/polymarket_orderbook_"
ARCHIVE_SUFFIX = ".parquet"
REPO_ROOT = Path(__file__).resolve().parents[3]
AUTHORIZED_OUTPUT_ROOT = (REPO_ROOT / "outputs" / "pmxt-ordering-audit").resolve()


class DownloadFailure(RuntimeError):
    """Raised when fewer than two predeclared dates can be resolved mechanically."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def object_key(hour: str) -> str:
    # candidate_hours performs the strict shape/date validation.
    candidate_hours(hour)
    return f"polymarket_orderbook_{hour}.parquet"


def download_url(hour: str) -> str:
    return f"https://{ARCHIVE_HOST}/{object_key(hour)}"


def ensure_authorized_output(path: Path) -> Path:
    resolved = path.resolve()
    if resolved != AUTHORIZED_OUTPUT_ROOT and AUTHORIZED_OUTPUT_ROOT not in resolved.parents:
        raise ValueError(
            f"output must remain beneath {AUTHORIZED_OUTPUT_ROOT}; got {resolved}"
        )
    return resolved


def validate_parquet(path: Path) -> dict[str, int]:
    """Open the footer and return row/row-group counts without scanning conclusions."""

    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT num_rows, num_row_groups FROM parquet_file_metadata(?)", [str(path)]
        ).fetchone()
    finally:
        connection.close()
    if row is None or int(row[0]) < 1 or int(row[1]) < 1:
        raise ValueError("Parquet footer reports no rows or row groups")
    return {"row_count": int(row[0]), "row_group_count": int(row[1])}


def _atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".part")
    payload = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _header_value(headers: dict[str, str], name: str) -> str | None:
    return headers.get(name.lower())


def _download_candidate(
    hour: str,
    destination: Path,
    *,
    connection_factory: Callable[..., http.client.HTTPSConnection] = (
        http.client.HTTPSConnection
    ),
) -> dict[str, Any]:
    started_at = utc_now()
    key = object_key(hour)
    url = download_url(hour)
    attempt: dict[str, Any] = {
        "candidate_hour": hour,
        "object_key": key,
        "download_url": url,
        "download_started_at": started_at,
        "http_method": "GET",
        "authenticated": False,
    }
    if destination.exists():
        attempt.update(
            {
                "download_ended_at": utc_now(),
                "http_status": None,
                "outcome": "REUSED_EXISTING_IMMUTABLE_FILE",
                "byte_length": destination.stat().st_size,
                "sha256": sha256_file(destination),
            }
        )
        return attempt

    temporary = destination.with_suffix(destination.suffix + ".part")
    connection = connection_factory(
        ARCHIVE_HOST,
        timeout=120,
        context=ssl.create_default_context(),
    )
    response: http.client.HTTPResponse | None = None
    try:
        connection.request(
            "GET",
            f"{ARCHIVE_PREFIX}{hour}{ARCHIVE_SUFFIX}",
            headers={"User-Agent": "polymarket-ai-bot-ip-002-research-audit/0.1"},
        )
        response = connection.getresponse()
        headers = {name.lower(): value for name, value in response.getheaders()}
        attempt.update(
            {
                "http_status": response.status,
                "etag": _header_value(headers, "etag"),
                "last_modified": _header_value(headers, "last-modified"),
                "content_length_header": _header_value(headers, "content-length"),
                "content_type": _header_value(headers, "content-type"),
            }
        )
        if response.status != 200:
            response.read(64 * 1024)
            attempt.update(
                {
                    "download_ended_at": utc_now(),
                    "outcome": "HTTP_UNAVAILABLE",
                    "reason": f"HTTP {response.status} {response.reason}",
                }
            )
            return attempt
        digest = hashlib.sha256()
        byte_length = 0
        with temporary.open("wb") as handle:
            while chunk := response.read(8 * 1024 * 1024):
                handle.write(chunk)
                digest.update(chunk)
                byte_length += len(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        expected_length = _header_value(headers, "content-length")
        if expected_length is not None and byte_length != int(expected_length):
            raise OSError(
                f"downloaded {byte_length} bytes, expected Content-Length {expected_length}"
            )
        os.replace(temporary, destination)
        attempt.update(
            {
                "download_ended_at": utc_now(),
                "outcome": "DOWNLOADED",
                "byte_length": byte_length,
                "sha256": digest.hexdigest(),
            }
        )
        return attempt
    except Exception as exc:
        if temporary.exists():
            temporary.unlink()
        attempt.update(
            {
                "download_ended_at": utc_now(),
                "outcome": "DOWNLOAD_FAILED",
                "reason": f"{type(exc).__name__}: {exc}",
            }
        )
        return attempt
    finally:
        if response is not None:
            response.close()
        connection.close()


def acquire_samples(
    output_directory: Path,
    *,
    requested_hours: tuple[str, ...] = PREDECLARED_SAMPLE_HOURS,
    downloader: Callable[[str, Path], dict[str, Any]] = _download_candidate,
    validator: Callable[[Path], dict[str, int]] = validate_parquet,
) -> dict[str, Any]:
    """Apply the predeclared same-date first-later-valid fallback mechanically."""

    output_directory = ensure_authorized_output(output_directory)
    raw_directory = output_directory / "raw"
    raw_directory.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "archive_host": ARCHIVE_HOST,
        "selection_rule": (
            "requested hour, then the first later valid hour on the same UTC date; "
            "never expand to another date"
        ),
        "requested_hours": list(requested_hours),
        "started_at": utc_now(),
        "samples": [],
    }
    valid_count = 0
    for requested in requested_hours:
        sample: dict[str, Any] = {
            "requested_hour": requested,
            "requested_object_key": object_key(requested),
            "attempts": [],
            "status": "MISSING",
        }
        for candidate in candidate_hours(requested):
            path = raw_directory / object_key(candidate)
            attempt = downloader(candidate, path)
            sample["attempts"].append(attempt)
            if attempt.get("outcome") not in {
                "DOWNLOADED",
                "REUSED_EXISTING_IMMUTABLE_FILE",
            }:
                continue
            try:
                parquet = validator(path)
            except Exception as exc:
                attempt["parquet_validation"] = "INVALID"
                attempt["parquet_validation_reason"] = f"{type(exc).__name__}: {exc}"
                continue
            actual_hash = sha256_file(path)
            if attempt.get("sha256") != actual_hash:
                attempt["parquet_validation"] = "HASH_MISMATCH"
                attempt["parquet_validation_reason"] = (
                    "file hash changed between acquisition and validation"
                )
                continue
            attempt["parquet_validation"] = "VALID"
            attempt.update(parquet)
            sample.update(
                {
                    "status": "VALID",
                    "actual_hour": candidate,
                    "actual_object_key": object_key(candidate),
                    "download_url": download_url(candidate),
                    "local_path": str(path),
                    "byte_length": path.stat().st_size,
                    "sha256": actual_hash,
                    "fallback_applied": candidate != requested,
                }
            )
            valid_count += 1
            break
        if sample["status"] != "VALID":
            sample["failure_reason"] = "no valid object from requested hour through 23 UTC"
        result["samples"].append(sample)
        _atomic_json(output_directory / "download-provenance.json", result)

    result["ended_at"] = utc_now()
    result["valid_sample_count"] = valid_count
    result["status"] = "READY" if valid_count >= 2 else "UNRESOLVED"
    _atomic_json(output_directory / "download-provenance.json", result)
    if valid_count < 2:
        raise DownloadFailure(
            "fewer than two predeclared sample dates produced a valid hourly Parquet"
        )
    return result
