import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(
    shutil.which("docker") is None, reason="the docker CLI is not available"
)


def compose_services(*arguments: str) -> dict[str, dict[str, object]]:
    # COMPOSE_PROFILES from the caller's shell (run.sh exports it) must not leak into the check.
    environment = {key: value for key, value in os.environ.items() if key != "COMPOSE_PROFILES"}
    result = subprocess.run(
        ["docker", "compose", *arguments, "config", "--format", "json"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip(f"docker compose is not usable here: {result.stderr.strip()}")
    services: dict[str, dict[str, object]] = json.loads(result.stdout)["services"]
    return services


def test_plain_compose_config_does_not_include_the_demo_service() -> None:
    assert set(compose_services()) == {
        "postgres",
        "rabbitmq",
        "migrate",
        "api",
        "relay",
        "consumer",
    }


def test_demo_profile_adds_the_demo_service_and_only_it() -> None:
    plain = set(compose_services())
    with_demo = compose_services("--profile", "demo")

    assert set(with_demo) - plain == {"demo"}
    assert with_demo["demo"]["profiles"] == ["demo"]


def test_only_the_demo_service_is_under_a_profile() -> None:
    services = compose_services("--profile", "demo")

    assert {name for name, service in services.items() if service.get("profiles")} == {"demo"}
