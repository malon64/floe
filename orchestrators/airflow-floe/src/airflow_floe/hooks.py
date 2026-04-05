"""Hooks for Airflow-Floe connector."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from .runtime import DagManifestContext, build_dag_manifest_context_or_empty

if TYPE_CHECKING:
    from .profile import FloeProfile


def load_manifest_context(
    manifest_path: str,
    *,
    config_override: str | None = None,
    default_config_path: str | None = None,
) -> DagManifestContext:
    return build_dag_manifest_context_or_empty(
        manifest_path=manifest_path,
        config_override=config_override,
        default_config_path=default_config_path,
    )


class FloeManifestHook:
    """Resolve connector runtime context from a Floe Airflow manifest.

    Optionally accepts a *profile_path* to enable profile-driven execution
    routing.  Call :meth:`get_runner_type` to read the runner intent from
    the profile before constructing a :class:`~airflow_floe.operators.FloeRunOperator`.

    Example::

        hook = FloeManifestHook(manifest_path, profile_path="/profiles/prod.yml")
        runner_type = hook.get_runner_type()  # "local", "kubernetes", or None
        context = hook.get_context()

        run_task = FloeRunOperator(
            task_id="floe_run",
            config_path=hook.get_config_path(),
            manifest_context=context,
            profile_path="/profiles/prod.yml",
        )
    """

    def __init__(
        self,
        manifest_path: str,
        *,
        config_override: str | None = None,
        default_config_path: str | None = None,
        profile_path: str | None = None,
    ) -> None:
        self.manifest_path = manifest_path
        self.config_override = config_override
        self.default_config_path = default_config_path
        self.profile_path = profile_path
        self._context: DagManifestContext | None = None
        self._profile: FloeProfile | None = None

    def get_context(self) -> DagManifestContext:
        if self._context is None:
            self._context = load_manifest_context(
                self.manifest_path,
                config_override=self.config_override,
                default_config_path=self.default_config_path,
            )
        return self._context

    def get_assets(self):
        return list(self.get_context().assets_by_entity.values())

    def get_entities(self) -> list[str]:
        return self.get_context().entity_names

    def get_config_path(self) -> str:
        return self.get_context().config_path

    def get_runner_type(self) -> str | None:
        """Return the execution runner type from the profile, or ``None``.

        Returns ``None`` when no *profile_path* was supplied or when the
        profile has no ``execution.runner.type`` field.  ``"local"`` and
        ``"kubernetes"`` are the recognized values.
        """
        if self.profile_path is None:
            return None
        if self._profile is None:
            from .profile import load_profile

            self._profile = load_profile(self.profile_path)
        return self._profile.runner_type

    @staticmethod
    def default_example_paths() -> tuple[str, str]:
        base = Path(__file__).resolve().parents[2]
        manifest = str(base / "example" / "manifest.airflow.json")
        config = str(base / "example" / "config.yml")
        return manifest, config
