"""Floe environment profile parser for Airflow connector.

Reads the T1/T2 profile YAML format (apiVersion: floe/v1,
kind: EnvironmentProfile) to extract the execution runner intent
and variable values for connector-side routing.

Routing model
-------------
The profile's ``execution.runner.type`` field is the authoritative signal
that the Airflow connector uses to decide *how* to dispatch a Floe run:

* ``"local"`` (or no profile / no execution section) — run via the existing
  local subprocess path using :class:`~airflow_floe.operators.FloeRunHook`.
* ``"kubernetes"`` — connector-layer Kubernetes adapter (not yet implemented;
  :class:`~airflow_floe.operators.FloeRunOperator` will raise
  :exc:`NotImplementedError` until a K8s adapter is wired in).

Variable resolution
-------------------
Profile variables are surfaced on :attr:`FloeProfile.variables` for
connector-level use (e.g. DAG-level config overrides).  Full variable
substitution into floe configs via the ``floe run`` CLI is a future
integration point (requires ``--profile`` support in the Floe CLI).

Example profile YAML (T1/T2 format)::

    apiVersion: floe/v1
    kind: EnvironmentProfile
    metadata:
      name: prod
      env: prod
    execution:
      runner:
        type: local
    variables:
      CATALOG: prod_catalog
      SCHEMA: prod_schema

Usage in a DAG::

    from airflow_floe import FloeRunOperator, load_profile

    profile = load_profile("/profiles/prod.yml")
    # FloeRunOperator will read the profile and route accordingly.
    run_task = FloeRunOperator(
        task_id="floe_run",
        config_path="/configs/config.yml",
        profile_path="/profiles/prod.yml",
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

try:
    import yaml as _yaml
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "PyYAML is required for Floe profile parsing. "
        "Install it with: pip install pyyaml"
    ) from _exc

#: Expected ``apiVersion`` value for environment profiles.
PROFILE_API_VERSION = "floe/v1"

#: Expected ``kind`` value for environment profiles.
PROFILE_KIND = "EnvironmentProfile"

# Recognized runner types — mirrors floe-core RunnerKind variants.
_KNOWN_RUNNER_TYPES = frozenset({"local", "kubernetes"})


@dataclass(frozen=True)
class FloeProfile:
    """Parsed Floe environment profile (T1/T2 format).

    Exposes the execution runner intent and variable values extracted from
    a profile YAML for connector-side routing decisions.

    Attributes:
        name: Profile name from ``metadata.name``.
        runner_type: Runner type from ``execution.runner.type``, or ``None``
            if the profile has no ``execution`` section.  Recognized values
            are ``"local"`` and ``"kubernetes"``.
        variables: Key-value pairs from the ``variables`` section.
        env: Optional environment label from ``metadata.env``.
    """

    name: str
    runner_type: str | None
    variables: dict[str, str] = field(default_factory=dict)
    env: str | None = None

    def is_local(self) -> bool:
        """Return ``True`` if this profile targets local execution.

        Profiles with no ``execution.runner.type`` default to local.
        """
        return self.runner_type is None or self.runner_type == "local"

    def is_kubernetes(self) -> bool:
        """Return ``True`` if this profile targets Kubernetes execution."""
        return self.runner_type == "kubernetes"


def load_profile(path: str) -> FloeProfile:
    """Load and parse a Floe environment profile YAML file.

    Reads the T1/T2 profile format and validates structure, ``apiVersion``,
    ``kind``, required fields, and runner type.

    Args:
        path: Filesystem path to the profile ``.yml`` / ``.yaml`` file.

    Returns:
        Parsed :class:`FloeProfile`.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the YAML is malformed, missing required fields, has
            an unrecognized ``apiVersion``/``kind``, or specifies an unknown
            runner type.
    """
    profile_path = Path(path)
    if not profile_path.exists():
        raise FileNotFoundError(f"profile file not found: {path}")

    raw = profile_path.read_text(encoding="utf-8")
    try:
        doc = _yaml.safe_load(raw)
    except _yaml.YAMLError as exc:
        raise ValueError(f"failed to parse profile YAML at {path}: {exc}") from exc

    if not isinstance(doc, dict):
        raise ValueError(
            f"profile YAML at {path} must be a mapping, got {type(doc).__name__}"
        )

    api_version = doc.get("apiVersion")
    if api_version != PROFILE_API_VERSION:
        raise ValueError(
            f"profile.apiVersion: expected \"{PROFILE_API_VERSION}\", "
            f"got \"{api_version}\""
        )

    kind = doc.get("kind")
    if kind != PROFILE_KIND:
        raise ValueError(
            f"profile.kind: expected \"{PROFILE_KIND}\", got \"{kind}\""
        )

    metadata = doc.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("profile.metadata is required and must be a mapping")

    name = metadata.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            "profile.metadata.name is required and must be a non-empty string"
        )

    env: str | None = None
    raw_env = metadata.get("env")
    if raw_env is not None:
        if not isinstance(raw_env, str):
            raise ValueError("profile.metadata.env must be a string")
        env = raw_env

    runner_type: str | None = None
    execution = doc.get("execution")
    if execution is not None:
        if not isinstance(execution, dict):
            raise ValueError("profile.execution must be a mapping")
        runner = execution.get("runner")
        if runner is not None:
            if not isinstance(runner, dict):
                raise ValueError("profile.execution.runner must be a mapping")
            rt = runner.get("type")
            if rt is not None:
                if not isinstance(rt, str):
                    raise ValueError("profile.execution.runner.type must be a string")
                if rt not in _KNOWN_RUNNER_TYPES:
                    raise ValueError(
                        f"profile.execution.runner.type: unknown runner \"{rt}\"; "
                        f"known runners: {', '.join(sorted(_KNOWN_RUNNER_TYPES))}"
                    )
                runner_type = rt

    variables: dict[str, str] = {}
    raw_vars = doc.get("variables")
    if raw_vars is not None:
        if not isinstance(raw_vars, dict):
            raise ValueError("profile.variables must be a mapping")
        for k, v in raw_vars.items():
            if not isinstance(k, str):
                raise ValueError(
                    f"profile.variables key must be a string, got {type(k).__name__}"
                )
            if not isinstance(v, str):
                raise ValueError(
                    f"profile.variables.{k} must be a string, "
                    f"got {type(v).__name__}"
                )
            variables[k] = v

    return FloeProfile(
        name=name,
        runner_type=runner_type,
        variables=dict(variables),
        env=env,
    )
