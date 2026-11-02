"""Download original TLC files using only the Python standard library."""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_file(path, file_format):
    if file_format == "parquet":
        with path.open("rb") as stream:
            first = stream.read(4)
            stream.seek(-4, 2)
            last = stream.read(4)
        if first != b"PAR1" or last != b"PAR1":
            raise ValueError(f"Not a Parquet file: {path}")
    else:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {"LocationID", "Borough", "Zone", "service_zone"}
            if not required.issubset(reader.fieldnames or []):
                raise ValueError(f"Unexpected lookup CSV header: {path}")
            if not any(reader):
                raise ValueError("Taxi zone lookup is empty")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2023-01")
    args = parser.parse_args()
    if len(args.month) != 7:
        parser.error("Month must be YYYY-MM")
    datetime.strptime(args.month, "%Y-%m")
    raw_dir = ROOT / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = raw_dir / f"manifest-{args.month}.json"
    previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    prior_files = {entry["name"]: entry for entry in previous.get("files", [])}
    sources = [
        (f"yellow_tripdata_{args.month}.parquet", "parquet",
         f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{args.month}.parquet"),
        ("taxi_zone_lookup.csv", "csv",
         "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"),
    ]
    entries = []
    for name, file_format, url in sources:
        target = raw_dir / name
        old = prior_files.get(name)
        # The lookup is shared by months; accept a matching recorded manifest.
        if target.exists() and old is None:
            for candidate in sorted(raw_dir.glob("manifest-*.json")):
                recorded = json.loads(candidate.read_text(encoding="utf-8"))
                old = next((item for item in recorded.get("files", []) if item["name"] == name), None)
                if old:
                    break
        if target.exists():
            if not old or old.get("source_url") != url or sha256(target) != old["sha256"]:
                raise ValueError(f"Existing file has no matching source/hash manifest: {target}. Preserve it before retrying.")
            validate_file(target, file_format)
            print(f"REUSE {name}: SHA-256 verified", flush=True)
            entries.append(old)
            continue
        print(f"DOWNLOAD {url}", flush=True)
        partial = target.with_suffix(target.suffix + ".part")
        with urlopen(Request(url, headers={"User-Agent": "BigDataCourse-Step1/1.0"}), timeout=60) as response:
            expected_bytes = response.headers.get("Content-Length")
            byte_count = 0
            next_notice = 8 * 1024 * 1024
            with partial.open("wb") as stream:
                while chunk := response.read(1024 * 1024):
                    stream.write(chunk)
                    byte_count += len(chunk)
                    if byte_count >= next_notice:
                        print(f"  {name}: {byte_count / 1024 / 1024:.0f} MiB", flush=True)
                        next_notice += 8 * 1024 * 1024
            if expected_bytes and byte_count != int(expected_bytes):
                raise ValueError(f"Incomplete download of {name}")
            validate_file(partial, file_format)
            entry = {
                "name": name, "format": file_format, "source_url": url,
                "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                "size_bytes": byte_count, "sha256": sha256(partial),
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
            }
        partial.replace(target)
        entries.append(entry)
        # Persist each successful file so an interrupted second download is resumable.
        checkpoint = {"schema_version": 1, "source_page": SOURCE_PAGE, "month": args.month, "files": entries}
        manifest_path.write_text(json.dumps(checkpoint, indent=2) + "\n", encoding="utf-8")
        print(f"SAVED {name}: {byte_count} bytes, sha256={entry['sha256']}", flush=True)
    manifest = {"schema_version": 1, "source_page": SOURCE_PAGE, "month": args.month, "files": entries}
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"MANIFEST {manifest_path}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
