from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.probes.pmxt_ordering_audit.core import sha256_file
from tools.probes.pmxt_ordering_audit.download import (
    AUTHORIZED_OUTPUT_ROOT,
    DownloadFailure,
    acquire_samples,
    download_url,
    ensure_authorized_output,
    object_key,
)


class DownloadSelectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        AUTHORIZED_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    def test_exact_public_url_shape(self) -> None:
        self.assertEqual(
            download_url("2026-05-01T12"),
            "https://r2v2.pmxt.dev/polymarket_orderbook_2026-05-01T12.parquet",
        )

    def test_first_later_valid_same_date_is_selected_and_provenance_hashed(self) -> None:
        calls: list[str] = []

        def downloader(hour: str, path: Path) -> dict:
            calls.append(hour)
            if hour == "2026-05-01T12":
                return {"candidate_hour": hour, "outcome": "HTTP_UNAVAILABLE", "http_status": 404}
            path.write_bytes(hour.encode())
            return {
                "candidate_hour": hour,
                "outcome": "DOWNLOADED",
                "sha256": sha256_file(path),
                "http_status": 200,
            }

        with tempfile.TemporaryDirectory(dir=AUTHORIZED_OUTPUT_ROOT) as temporary:
            result = acquire_samples(
                Path(temporary),
                requested_hours=("2026-05-01T12", "2026-06-15T12"),
                downloader=downloader,
                validator=lambda path: {"row_count": 1, "row_group_count": 1},
            )
            first = result["samples"][0]
            self.assertEqual(first["actual_hour"], "2026-05-01T13")
            self.assertTrue(first["fallback_applied"])
            self.assertEqual(first["sha256"], sha256_file(Path(first["local_path"])))
            self.assertTrue((Path(temporary) / "download-provenance.json").is_file())
        self.assertEqual(calls[:2], ["2026-05-01T12", "2026-05-01T13"])
        self.assertNotIn("2026-05-02T00", calls)

    def test_fewer_than_two_valid_dates_stops_unresolved(self) -> None:
        def downloader(hour: str, path: Path) -> dict:
            if hour.startswith("2026-05-01T") and hour.endswith("12"):
                path.write_bytes(b"valid")
                return {
                    "candidate_hour": hour,
                    "outcome": "DOWNLOADED",
                    "sha256": sha256_file(path),
                    "http_status": 200,
                }
            return {"candidate_hour": hour, "outcome": "HTTP_UNAVAILABLE", "http_status": 404}

        with tempfile.TemporaryDirectory(dir=AUTHORIZED_OUTPUT_ROOT) as temporary:
            with self.assertRaises(DownloadFailure):
                acquire_samples(
                    Path(temporary),
                    requested_hours=("2026-05-01T12", "2026-06-15T12"),
                    downloader=downloader,
                    validator=lambda path: {"row_count": 1, "row_group_count": 1},
                )

    def test_output_outside_gitignored_authorized_root_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                ensure_authorized_output(Path(temporary))

    def test_object_key_rejects_non_predeclared_shape(self) -> None:
        with self.assertRaises(ValueError):
            object_key("../../secret")


if __name__ == "__main__":
    unittest.main()
