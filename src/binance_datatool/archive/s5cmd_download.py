"""s5cmd-backed parallel download from data.binance.vision.

Provides bulk ZIP download using ``s5cmd cp`` with connection pooling
and parallel requests — significantly faster than aiohttp for batch
downloads of many files.

Public S3 bucket: ``s3://data.binance.vision/`` with anonymous access.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

_S5CMD_BIN = shutil.which("s5cmd") or "s5cmd"


def _check_s5cmd() -> bool:
    """Check if s5cmd is available on the system."""
    return shutil.which("s5cmd") is not None


def download_files(
    s3_keys: list[str],
    *,
    concurrency: int = 10,
    target_dir: Path | None = None,
) -> list[Path]:
    """Download multiple S3 ZIP files in parallel using s5cmd.

    Files are downloaded to a temporary directory (or custom target_dir)
    preserving the S3 key structure. Returns a list of local file paths
    in the same order as s3_keys.

    Args:
        s3_keys: S3 object keys (e.g. ``data/spot/daily/klines/BTCUSDT/1d/...zip``).
        concurrency: Number of parallel download connections.
        target_dir: Custom download directory. Uses ``tempfile.mkdtemp()`` if None.

    Returns:
        List of ``Path`` objects pointing to downloaded ZIP files.

    Raises:
        RuntimeError: If s5cmd is not installed or download fails.
        subprocess.CalledProcessError: If s5cmd exits with an error code.
    """
    if not _check_s5cmd():
        raise RuntimeError("s5cmd not found. Install: https://github.com/peak/s5cmd")

    if not s3_keys:
        return []

    tmpdir: str | None = None
    try:
        tmpdir = str(target_dir) if target_dir else tempfile.mkdtemp(prefix="s5cmd_archive_")

        # Build a command file for s5cmd run (batch mode)
        cmd_file = Path(tmpdir) / "_s5cmd_commands.txt"
        with open(cmd_file, "w") as f:
            for key in s3_keys:
                s3_uri = f"s3://data.binance.vision/{key}"
                f.write(f"cp --flatten {s3_uri} {tmpdir}/\n")

        args: list[str] = [_S5CMD_BIN, "--no-sign-request", "run", str(cmd_file)]

        result = subprocess.run(args, capture_output=True, text=True, timeout=300)
        if result.returncode != 0 and "ERROR" in result.stderr:
            raise RuntimeError(f"s5cmd run failed: {result.stderr.strip()}")

        # s5cmd --flatten places files in the target directory
        # Match downloaded files to s3_keys by filename
        downloaded = list(Path(tmpdir).glob("*.zip"))
        if not downloaded:
            return []

        # Create a mapping from key filename to local path
        key_to_path: dict[str, Path] = {}
        for dp in downloaded:
            key_to_path[dp.name] = dp

        # Return paths in the same order as s3_keys
        result_paths: list[Path] = []
        for key in s3_keys:
            fname = key.rpartition("/")[2]
            if fname in key_to_path:
                result_paths.append(key_to_path[fname])

        return result_paths

    except subprocess.TimeoutExpired as e:
        if tmpdir and not target_dir:
            import shutil as _shutil

            _shutil.rmtree(tmpdir, ignore_errors=True)
        raise RuntimeError("s5cmd download timed out (300s)") from e
    except Exception:
        if tmpdir and not target_dir:
            import shutil as _shutil

            _shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def read_zips(local_paths: list[Path]) -> Iterator[tuple[str, str]]:
    """Read CSV content from downloaded ZIP files.

    Yields ``(filename, csv_text)`` tuples for each ZIP file.
    Skips files that are not valid ZIPs.
    """
    for path in local_paths:
        try:
            with zipfile.ZipFile(path, "r") as zf:
                csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                if csv_name:
                    text = zf.read(csv_name).decode()
                    yield (csv_name, text)
        except (zipfile.BadZipFile, OSError):
            continue


def download_and_parse(
    s3_keys: list[str],
    *,
    symbol: str,
    data_type: str,
    interval: str | None = None,
    concurrency: int = 10,
) -> list[list[dict[str, object]]]:
    """Download archive ZIPs with s5cmd and parse CSV rows.

    Full pipeline: s5cmd bulk download → unzip → CSV parse.
    Returns parsed rows grouped by file.

    Args:
        s3_keys: S3 object keys.
        symbol: Trading pair for metadata injection.
        data_type: Data type for column mapping.
        interval: Kline interval (required for klines-like types).
        concurrency: s5cmd download concurrency.

    Returns:
        List of parsed row lists, one per downloaded file.
    """
    from binance_datatool.dlt.resources.binance_archive import _parse_csv_rows

    local_paths = download_files(s3_keys, concurrency=concurrency)
    results: list[list[dict[str, object]]] = []
    for _csv_name, text in read_zips(local_paths):
        rows = _parse_csv_rows(text, data_type, symbol, interval)
        if rows:
            results.append(rows)
    return results
