import gzip
import importlib
import io
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from contextlib import redirect_stdout
from unittest.mock import patch

from graph_drawings import compact
from graph_drawings.shards import write_flag_shard
from graph_drawings.status import connect_db, init_schema, insert_shard_manifest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


class _BoundedReader:
    def __init__(self, data: bytes):
        self.data = data
        self.offset = 0
        self.read_sizes = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, size=-1):
        if size < 0:
            raise AssertionError("gzip record iterator used an unbounded read")
        self.read_sizes.append(size)
        chunk = self.data[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


class StreamIoTest(unittest.TestCase):
    def test_gzip_records_roundtrip_and_bounded_reads(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "records.gz"
            compact.write_gzip_records(path, [b"first", b"second"])
            self.assertEqual(list(compact.iter_gzip_records(path)), [b"first", b"second"])

        reader = _BoundedReader(b"\x03one\x06second")
        with patch.object(compact.gzip, "open", return_value=reader):
            self.assertEqual(list(compact.iter_gzip_records(Path("ignored.gz"))), [b"one", b"second"])
        self.assertTrue(reader.read_sizes)
        self.assertTrue(all(size >= 0 for size in reader.read_sizes))

    def test_truncated_record_length_and_payload_fail(self):
        with TemporaryDirectory() as directory:
            directory_path = Path(directory)
            for name, payload, message in (
                ("length.gz", b"\x80", "length"),
                ("payload.gz", b"\x05abc", "record"),
            ):
                path = directory_path / name
                with gzip.open(path, "wb") as output:
                    output.write(payload)
                with self.assertRaisesRegex(ValueError, message):
                    list(compact.iter_gzip_records(path))

    def test_export_reads_final_flag_shards_and_preflights_before_overwrite(self):
        export_drawings = importlib.import_module("export_drawings")
        with TemporaryDirectory() as directory:
            root = Path(directory)
            runs_root = root / "runs"
            for mode, export_format, filename, flags in (
                (
                    "crossing_pair_flags",
                    "crossing-pairs",
                    "crossing_pair_flags.txt",
                    ["4 0 1 1234", "4 0 0", "4 0 1 1234"],
                ),
                (
                    "four_graph_flags",
                    "four-graph",
                    "four_graph_flags.txt",
                    ["4 0 1 1234", "4 0 0", "4 0 1 1234"],
                ),
            ):
                run_dir = runs_root / mode
                run_dir.mkdir(parents=True)
                (run_dir / "run_config.json").write_text(
                    '{"total_steps": 1, "final_output_mode": "%s"}' % mode,
                    encoding="utf-8",
                )
                shard = run_dir / "shards" / "stage-1" / "reduced" / "aa" / ("a" * 64 + ".gds.gz")
                write_flag_shard(shard, flags)
                db = connect_db(run_dir / "coordinator.sqlite")
                init_schema(db)
                insert_shard_manifest(
                    db,
                    shard_path=str(shard.relative_to(run_dir)),
                    stage_index=1,
                    bucket_hash="a" * 64,
                    kind="reduced",
                    source_job_key=None,
                    source_worker="test",
                    record_count=len(flags),
                    byte_count=shard.stat().st_size,
                )
                db.commit()
                db.close()

                with patch.object(export_drawings.config, "RUNS_ROOT", runs_root), patch.object(
                    sys, "argv", ["export_drawings.py", "--run-id", mode, "--format", export_format]
                ):
                    stdout = io.StringIO()
                    with redirect_stdout(stdout):
                        export_drawings.main()
                output = run_dir / "outputs" / filename
                self.assertEqual(output.read_text(encoding="utf-8").splitlines(), flags)
                self.assertIn("stored", stdout.getvalue())
                summary = json.loads((run_dir / "outputs" / "summary.json").read_text(encoding="utf-8"))
                self.assertIsNone(summary["canonical_drawings"])
                self.assertEqual(summary["final_output_mode"], mode)

            run_dir = runs_root / "crossing_pair_flags"
            output = root / "existing.txt"
            output.write_text("keep me\n", encoding="utf-8")
            with patch.object(export_drawings.config, "RUNS_ROOT", runs_root), patch.object(
                sys,
                "argv",
                ["export_drawings.py", "--run-id", "crossing_pair_flags", "--format", "jsonl", "--output", str(output)],
            ):
                with self.assertRaises(SystemExit):
                    export_drawings.main()
            self.assertEqual(output.read_text(encoding="utf-8"), "keep me\n")

            with patch.object(export_drawings.config, "RUNS_ROOT", runs_root), patch.object(
                sys,
                "argv",
                [
                    "export_drawings.py",
                    "--run-id",
                    "crossing_pair_flags",
                    "--format",
                    "crossing-pairs",
                    "--vertex-label-offset",
                    "2",
                    "--output",
                    str(output),
                ],
            ):
                with self.assertRaises(SystemExit):
                    export_drawings.main()
            self.assertEqual(output.read_text(encoding="utf-8"), "keep me\n")


if __name__ == "__main__":
    unittest.main()
