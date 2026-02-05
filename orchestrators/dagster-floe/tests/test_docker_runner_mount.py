from __future__ import annotations

from pathlib import Path

from floe_dagster.runner import _infer_mount_root_for_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_mount_root_defaults_to_config_dir(tmp_path: Path) -> None:
    cfg = tmp_path / "configs" / "demo.yml"
    _write(
        cfg,
        """
version: "0.1"
report: { path: ./report }
entities:
  - name: demo
    source: { format: csv, path: ./in }
    sink: { accepted: { format: parquet, path: ./out/accepted/demo } }
    policy: { severity: warn }
    schema: { columns: [{ name: id, type: string }] }
""",
    )
    assert _infer_mount_root_for_config(cfg) == cfg.parent


def test_mount_root_moves_up_for_parent_refs(tmp_path: Path) -> None:
    cfg = tmp_path / "configs" / "demo.yml"
    _write(
        cfg,
        """
version: "0.1"
entities:
  - name: demo
    source: { format: csv, path: ../data/in }
    sink: { accepted: { format: parquet, path: ../data/out } }
    policy: { severity: warn }
    schema: { columns: [{ name: id, type: string }] }
""",
    )
    assert _infer_mount_root_for_config(cfg) == tmp_path


def test_mount_root_moves_up_multiple_levels(tmp_path: Path) -> None:
    cfg = tmp_path / "a" / "b" / "configs" / "demo.yml"
    _write(
        cfg,
        """
version: "0.1"
entities:
  - name: demo
    source: { format: csv, path: ../../data/in }
    sink: { accepted: { format: parquet, path: ../../data/out } }
    policy: { severity: warn }
    schema: { columns: [{ name: id, type: string }] }
""",
    )
    assert _infer_mount_root_for_config(cfg) == tmp_path / "a"

