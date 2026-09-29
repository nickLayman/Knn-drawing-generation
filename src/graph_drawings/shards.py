"""Compressed shard record helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .compact import iter_gzip_records, write_gzip_records


HASH_BYTES = 64
COMPOUND_RAW_MAGIC = b"GCR1"
COMPOUND_FLAG_MAGIC = b"GCF1"


@dataclass(frozen=True, slots=True)
class ShardRecord:
    report_hash: str
    work_hash: str
    compact_payload: bytes


@dataclass(frozen=True, slots=True)
class CompoundRawRecord:
    bucket_hash: str
    compact_payload: bytes


@dataclass(frozen=True, slots=True)
class CompoundFlagRecord:
    flag_hash: str
    invariant_key: str
    flag: str


def bucket_prefix(bucket_hash: str) -> str:
    return bucket_hash[:2]


def crossing_folder(crossing_count: int | None) -> str | None:
    if crossing_count is None:
        return None
    return f"crossings-{crossing_count}"


def bucket_root(
    run_dir: Path,
    stage_index: int,
    kind: str,
    bucket_hash: str,
    crossing_count: int | None = None,
) -> Path:
    root = run_dir / "shards" / f"stage-{stage_index}" / kind
    crossing = crossing_folder(crossing_count)
    if crossing:
        root = root / crossing
    return root / bucket_prefix(bucket_hash)


def incoming_shard_path(
    run_dir: Path,
    stage_index: int,
    bucket_hash: str,
    worker_id: str,
    job_hash: str,
    crossing_count: int | None = None,
) -> Path:
    safe_worker = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in worker_id)
    return bucket_root(run_dir, stage_index, "incoming", bucket_hash, crossing_count) / bucket_hash / f"{safe_worker}-{job_hash}.gds.gz"


def incoming_partition_shard_path(
    run_dir: Path,
    stage_index: int,
    partition_hash: str,
    worker_id: str,
    flush_hash: str,
) -> Path:
    safe_worker = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in worker_id)
    return run_dir / "shards" / f"stage-{stage_index}" / "incoming" / partition_hash / f"{safe_worker}-{flush_hash}.gds.gz"


def reduced_shard_path(run_dir: Path, stage_index: int, bucket_hash: str, crossing_count: int | None = None) -> Path:
    return bucket_root(run_dir, stage_index, "reduced", bucket_hash, crossing_count) / f"{bucket_hash}.gds.gz"


def encode_reduced_record(record: ShardRecord) -> bytes:
    return record.report_hash.encode("ascii") + record.work_hash.encode("ascii") + record.compact_payload


def decode_reduced_record(payload: bytes) -> ShardRecord:
    if len(payload) < 2 * HASH_BYTES:
        raise ValueError("reduced shard record is too short")
    return ShardRecord(
        report_hash=payload[:HASH_BYTES].decode("ascii"),
        work_hash=payload[HASH_BYTES : 2 * HASH_BYTES].decode("ascii"),
        compact_payload=payload[2 * HASH_BYTES :],
    )


def encode_compound_raw_record(record: CompoundRawRecord) -> bytes:
    return COMPOUND_RAW_MAGIC + record.bucket_hash.encode("ascii") + record.compact_payload


def decode_compound_raw_record(payload: bytes, fallback_bucket_hash: str | None = None) -> CompoundRawRecord:
    if payload.startswith(COMPOUND_RAW_MAGIC):
        offset = len(COMPOUND_RAW_MAGIC)
        if len(payload) < offset + HASH_BYTES:
            raise ValueError("compound raw shard record is too short")
        return CompoundRawRecord(
            bucket_hash=payload[offset : offset + HASH_BYTES].decode("ascii"),
            compact_payload=payload[offset + HASH_BYTES :],
        )
    if fallback_bucket_hash is None:
        raise ValueError("raw shard record has no embedded bucket hash")
    return CompoundRawRecord(bucket_hash=fallback_bucket_hash, compact_payload=payload)


def encode_compound_flag_record(record: CompoundFlagRecord) -> bytes:
    invariant = record.invariant_key.encode("utf-8")
    return (
        COMPOUND_FLAG_MAGIC
        + record.flag_hash.encode("ascii")
        + len(invariant).to_bytes(4, "big")
        + invariant
        + record.flag.encode("utf-8")
    )


def decode_compound_flag_record(payload: bytes) -> CompoundFlagRecord:
    if payload.startswith(COMPOUND_FLAG_MAGIC):
        offset = len(COMPOUND_FLAG_MAGIC)
        if len(payload) < offset + HASH_BYTES + 4:
            raise ValueError("compound flag shard record is too short")
        flag_hash = payload[offset : offset + HASH_BYTES].decode("ascii")
        offset += HASH_BYTES
        invariant_len = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
        if len(payload) < offset + invariant_len:
            raise ValueError("compound flag shard record has truncated invariant key")
        invariant_key = payload[offset : offset + invariant_len].decode("utf-8")
        flag = payload[offset + invariant_len :].decode("utf-8")
        return CompoundFlagRecord(flag_hash=flag_hash, invariant_key=invariant_key, flag=flag)
    flag = payload.decode("utf-8")
    import hashlib

    return CompoundFlagRecord(flag_hash=hashlib.sha256(payload).hexdigest(), invariant_key="", flag=flag)


def write_raw_shard(path: Path, compact_payloads: list[bytes]) -> int:
    write_gzip_records(path, compact_payloads)
    return path.stat().st_size


def write_compound_raw_shard(path: Path, records: list[CompoundRawRecord]) -> int:
    write_gzip_records(path, [encode_compound_raw_record(record) for record in records])
    return path.stat().st_size


def iter_raw_shard(path: Path):
    yield from iter_gzip_records(path)


def iter_compound_raw_shard(path: Path, fallback_bucket_hash: str | None = None):
    for payload in iter_gzip_records(path):
        yield decode_compound_raw_record(payload, fallback_bucket_hash)


def write_flag_shard(path: Path, flags: list[str]) -> int:
    write_gzip_records(path, [flag.encode("utf-8") for flag in flags])
    return path.stat().st_size


def write_compound_flag_shard(path: Path, records: list[CompoundFlagRecord]) -> int:
    write_gzip_records(path, [encode_compound_flag_record(record) for record in records])
    return path.stat().st_size


def iter_flag_shard(path: Path):
    for payload in iter_gzip_records(path):
        yield payload.decode("utf-8")


def iter_compound_flag_shard(path: Path):
    for payload in iter_gzip_records(path):
        yield decode_compound_flag_record(payload)


def write_reduced_shard(path: Path, records: list[ShardRecord]) -> int:
    write_gzip_records(path, [encode_reduced_record(record) for record in records])
    return path.stat().st_size


def iter_reduced_shard(path: Path):
    for payload in iter_gzip_records(path):
        yield decode_reduced_record(payload)
