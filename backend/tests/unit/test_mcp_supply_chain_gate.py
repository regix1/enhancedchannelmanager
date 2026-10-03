"""Regression coverage for the MCP publication supply-chain gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "check_mcp_supply_chain.py"


def _load_gate():
    spec = importlib.util.spec_from_file_location("check_mcp_supply_chain", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _copy_policy_files(gate, destination_root: Path) -> None:
    for path in gate.POLICY_FILES:
        destination = destination_root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            (REPO_ROOT / path).read_text(encoding="utf-8"), encoding="utf-8"
        )


def test_repository_satisfies_mcp_publication_policy():
    gate = _load_gate()
    assert gate.check_repository(REPO_ROOT) == []


def test_policy_pins_mcp_to_reviewed_alpine_base():
    gate = _load_gate()

    dockerfile = (REPO_ROOT / "mcp-server/Dockerfile").read_text(encoding="utf-8")
    assert dockerfile.splitlines()[0] == f"FROM {gate.MCP_BASE_IMAGE}"
    assert gate.check_repository(REPO_ROOT) == []


def test_policy_enforces_fixed_mcp_openssl_package_floor(tmp_path):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)
    dockerfile = tmp_path / "mcp-server/Dockerfile"
    contents = dockerfile.read_text(encoding="utf-8")

    assert gate.MCP_OPENSSL_MINIMUM == "3.5.8-r0"
    for package in ("libcrypto3", "libssl3"):
        required = f"'{package}>={gate.MCP_OPENSSL_MINIMUM}'"
        assert required in contents
        dockerfile.write_text(
            contents.replace(required, f"'{package}>=3.5.7-r0'", 1)
        )

        failures = gate.check_repository(tmp_path)

        assert any("OpenSSL package floor" in failure for failure in failures), failures
        dockerfile.write_text(contents, encoding="utf-8")


def test_policy_rejects_comment_only_mcp_openssl_package_floors(tmp_path):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)
    dockerfile = tmp_path / "mcp-server/Dockerfile"
    contents = dockerfile.read_text(encoding="utf-8")

    comments = []
    for package in ("libcrypto3", "libssl3"):
        required = f"'{package}>={gate.MCP_OPENSSL_MINIMUM}'"
        assert required in contents
        contents = contents.replace(required, package, 1)
        comments.append(f"# Retain reviewed floor {required}")
    dockerfile.write_text("\n".join(comments) + "\n" + contents, encoding="utf-8")

    failures = gate.check_repository(tmp_path)

    assert sum("OpenSSL package floor" in failure for failure in failures) == 2, failures


def test_policy_rejects_a_different_digest_pinned_mcp_base(tmp_path):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)
    dockerfile = tmp_path / "mcp-server/Dockerfile"
    contents = dockerfile.read_text(encoding="utf-8")
    assert gate.MCP_BASE_IMAGE in contents
    dockerfile.write_text(
        contents.replace(gate.MCP_BASE_IMAGE, "python:3.12-slim@sha256:" + "0" * 64),
        encoding="utf-8",
    )

    failures = gate.check_repository(tmp_path)

    assert any("reviewed Alpine base digest" in failure for failure in failures), failures


def test_policy_fails_closed_when_an_image_input_is_missing(tmp_path):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)
    (tmp_path / "Dockerfile").unlink()

    with pytest.raises(FileNotFoundError):
        gate.check_repository(tmp_path)


def test_policy_rejects_a_floating_root_base(tmp_path):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)
    dockerfile = tmp_path / "Dockerfile"
    contents = dockerfile.read_text(encoding="utf-8")
    pinned = "node:20-alpine@sha256:"
    assert pinned in contents
    dockerfile.write_text(
        contents.replace(pinned, "node:20-alpine # sha256:", 1), encoding="utf-8"
    )

    failures = gate.check_repository(tmp_path)

    assert any("digest-pinned FROM" in failure for failure in failures), failures


@pytest.mark.parametrize(
    ("relative_path", "old", "new", "expected"),
    [
        (
            "mcp-server/Dockerfile",
            "python:3.12-alpine@sha256:",
            "python:3.12-alpine # sha256:",
            "reviewed Alpine base digest",
        ),
        (
            "Dockerfile",
            "RUN npm ci",
            "RUN npm install",
            "npm production build",
        ),
    ],
)
def test_policy_mutations_fail_closed(tmp_path, relative_path, old, new, expected):
    gate = _load_gate()
    _copy_policy_files(gate, tmp_path)

    target = tmp_path / relative_path
    original = target.read_text(encoding="utf-8")
    assert old in original, f"mutation trigger missing from {relative_path}"
    target.write_text(original.replace(old, new, 1), encoding="utf-8")

    failures = gate.check_repository(tmp_path)
    assert any(expected in failure for failure in failures), failures
