from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.profile import FloeProfile, load_profile  # noqa: E402


def _write_profile(tmp: Path, content: str) -> str:
    p = tmp / "profile.yml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


class FloeProfileTests(unittest.TestCase):
    # ------------------------------------------------------------------
    # load_profile — happy paths
    # ------------------------------------------------------------------

    def test_load_profile_local_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: dev
                  env: dev
                execution:
                  runner:
                    type: local
                """,
            )
            profile = load_profile(path)
            self.assertEqual(profile.name, "dev")
            self.assertEqual(profile.runner_type, "local")
            self.assertEqual(profile.env, "dev")
            self.assertTrue(profile.is_local())
            self.assertFalse(profile.is_kubernetes())

    def test_load_profile_kubernetes_runner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: prod
                execution:
                  runner:
                    type: kubernetes
                """,
            )
            profile = load_profile(path)
            self.assertEqual(profile.runner_type, "kubernetes")
            self.assertTrue(profile.is_kubernetes())
            self.assertFalse(profile.is_local())

    def test_load_profile_without_execution_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: minimal
                """,
            )
            profile = load_profile(path)
            self.assertIsNone(profile.runner_type)
            self.assertTrue(profile.is_local())
            self.assertFalse(profile.is_kubernetes())

    def test_load_profile_with_variables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: staging
                variables:
                  CATALOG: staging_catalog
                  SCHEMA: staging_schema
                """,
            )
            profile = load_profile(path)
            self.assertEqual(profile.variables, {"CATALOG": "staging_catalog", "SCHEMA": "staging_schema"})

    def test_load_profile_empty_variables_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: dev
                variables: {}
                """,
            )
            profile = load_profile(path)
            self.assertEqual(profile.variables, {})

    def test_load_profile_full(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: prod
                  description: Production profile
                  env: prod
                  tags:
                    - production
                execution:
                  runner:
                    type: local
                variables:
                  CATALOG: prod_catalog
                  SCHEMA: prod_schema
                validation:
                  strict: true
                """,
            )
            profile = load_profile(path)
            self.assertEqual(profile.name, "prod")
            self.assertEqual(profile.runner_type, "local")
            self.assertEqual(profile.env, "prod")
            self.assertEqual(profile.variables["CATALOG"], "prod_catalog")

    # ------------------------------------------------------------------
    # load_profile — error paths
    # ------------------------------------------------------------------

    def test_load_profile_missing_file_raises(self) -> None:
        with self.assertRaises(FileNotFoundError) as cm:
            load_profile("/nonexistent/path/profile.yml")
        self.assertIn("not found", str(cm.exception))

    def test_load_profile_invalid_yaml_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "profile.yml"
            p.write_text("apiVersion: floe/v1\nkey: [unclosed", encoding="utf-8")
            with self.assertRaises(ValueError) as cm:
                load_profile(str(p))
            self.assertIn("parse profile YAML", str(cm.exception))

    def test_load_profile_wrong_api_version_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v99
                kind: EnvironmentProfile
                metadata:
                  name: dev
                """,
            )
            with self.assertRaises(ValueError) as cm:
                load_profile(path)
            self.assertIn("apiVersion", str(cm.exception))

    def test_load_profile_wrong_kind_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: SomethingElse
                metadata:
                  name: dev
                """,
            )
            with self.assertRaises(ValueError) as cm:
                load_profile(path)
            self.assertIn("kind", str(cm.exception))

    def test_load_profile_missing_metadata_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                """,
            )
            with self.assertRaises(ValueError) as cm:
                load_profile(path)
            self.assertIn("metadata", str(cm.exception))

    def test_load_profile_empty_name_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: "   "
                """,
            )
            with self.assertRaises(ValueError) as cm:
                load_profile(path)
            self.assertIn("name", str(cm.exception))

    def test_load_profile_unknown_runner_type_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_profile(
                Path(tmp),
                """\
                apiVersion: floe/v1
                kind: EnvironmentProfile
                metadata:
                  name: dev
                execution:
                  runner:
                    type: databricks
                """,
            )
            with self.assertRaises(ValueError) as cm:
                load_profile(path)
            self.assertIn("databricks", str(cm.exception))

    # ------------------------------------------------------------------
    # FloeProfile helpers
    # ------------------------------------------------------------------

    def test_is_local_true_when_runner_type_none(self) -> None:
        p = FloeProfile(name="test", runner_type=None)
        self.assertTrue(p.is_local())

    def test_is_local_true_when_runner_type_local(self) -> None:
        p = FloeProfile(name="test", runner_type="local")
        self.assertTrue(p.is_local())

    def test_is_local_false_for_kubernetes(self) -> None:
        p = FloeProfile(name="test", runner_type="kubernetes")
        self.assertFalse(p.is_local())

    def test_is_kubernetes_true(self) -> None:
        p = FloeProfile(name="test", runner_type="kubernetes")
        self.assertTrue(p.is_kubernetes())

    def test_is_kubernetes_false_for_local(self) -> None:
        p = FloeProfile(name="test", runner_type="local")
        self.assertFalse(p.is_kubernetes())


if __name__ == "__main__":
    unittest.main()
