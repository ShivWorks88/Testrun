"""Immutable raw archive. Objects are written once (if_generation_match=0) and the bucket's
retention policy (Bucket Lock when locked) prevents deletion or overwrite."""
from __future__ import annotations

import hashlib
import io
import json
from datetime import date
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq


def object_name(dataset: str, business_date: date, batch_id: str, suffix: str = "parquet") -> str:
    return f"raw/{dataset}/load_date={business_date.isoformat()}/{batch_id}.{suffix}"


def to_parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


class Archive(Protocol):
    def uri(self, name: str) -> str: ...
    def put(self, name: str, data: bytes, content_type: str, metadata: dict | None = None) -> str: ...
    def get(self, name: str) -> bytes: ...
    def list(self, prefix: str) -> list[str]: ...


class GcsArchive:
    def __init__(self, client, bucket: str):
        self.bucket = client.bucket(bucket)
        self.name = bucket

    def uri(self, name: str) -> str:
        return f"gs://{self.name}/{name}"

    def put(self, name: str, data: bytes, content_type: str, metadata: dict | None = None) -> str:
        blob = self.bucket.blob(name)
        blob.metadata = {k: str(v) for k, v in (metadata or {}).items()}
        blob.upload_from_string(data, content_type=content_type, if_generation_match=0)  # never overwrite
        return self.uri(name)

    def get(self, name: str) -> bytes:
        return self.bucket.blob(name).download_as_bytes()

    def list(self, prefix: str) -> list[str]:
        return sorted(b.name for b in self.bucket.list_blobs(prefix=prefix))


class LocalArchive:
    """Filesystem archive for tests and offline development. Same write-once semantics."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def uri(self, name: str) -> str:
        return f"file://{self.root / name}"

    def put(self, name: str, data: bytes, content_type: str, metadata: dict | None = None) -> str:
        p = self.root / name
        if p.exists():
            raise FileExistsError(f"archive object exists (write-once): {name}")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return self.uri(name)

    def get(self, name: str) -> bytes:
        return (self.root / name).read_bytes()

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())


def write_batch(archive: Archive, table: pa.Table, manifest: dict, dataset: str, business_date: date,
                batch_id: str) -> tuple[str, str]:
    """Archive the extract (Parquet) and its manifest (control totals, checksum). Returns both URIs."""
    data = to_parquet_bytes(table)
    sha = hashlib.sha256(data).hexdigest()
    manifest = {**manifest, "rows": table.num_rows, "sha256": sha, "format": "parquet"}
    parquet_uri = archive.put(object_name(dataset, business_date, batch_id), data, "application/octet-stream",
                              {"batch_id": batch_id, "rows": table.num_rows, "sha256": sha})
    manifest_uri = archive.put(object_name(dataset, business_date, batch_id, "manifest.json"),
                               json.dumps(manifest, indent=2, default=str).encode(), "application/json",
                               {"batch_id": batch_id})
    return parquet_uri, manifest_uri


def read_batch(archive: Archive, dataset: str, business_date: date, batch_id: str) -> tuple[pa.Table, dict]:
    stored = archive.get(object_name(dataset, business_date, batch_id))
    manifest = json.loads(archive.get(object_name(dataset, business_date, batch_id, "manifest.json")))
    if hashlib.sha256(stored).hexdigest() != manifest["sha256"]:
        raise RuntimeError(f"archive checksum mismatch for {batch_id}")
    return pq.read_table(io.BytesIO(stored)), manifest


def latest_batch_id(archive: Archive, dataset: str, business_date: date) -> str | None:
    prefix = f"raw/{dataset}/load_date={business_date.isoformat()}/"
    names = [n.rsplit("/", 1)[-1] for n in archive.list(prefix) if n.endswith(".parquet")]
    return sorted(names)[-1][: -len(".parquet")] if names else None
