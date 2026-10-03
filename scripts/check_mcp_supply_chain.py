#!/usr/bin/env python3
"""Fail closed when the ECM and MCP container image inputs drift."""

from __future__ import annotations

import re
import shlex
import sys
from pathlib import Path


POLICY_FILES = (
    Path("Dockerfile"),
    Path("mcp-server/Dockerfile"),
)

MCP_BASE_IMAGE = "python:3.12-alpine@sha256:" + "".join(
    (
        "d09d15e6",
        "0962ca36",
        "5d1cd544",
        "a48773ba",
        "c9d33f2f",
        "b1b00f2a",
        "a0deec78",
        "ade7dc31",
    )
)
MCP_OPENSSL_MINIMUM = "3.5.8-r0"

_FROM = re.compile(r"^FROM\s+(\S+)", re.MULTILINE)


def _has_apk_add_argument(dockerfile: str, required: str) -> bool:
    instruction_lines: list[str] = []
    for raw_line in dockerfile.splitlines():
        line = raw_line.strip()
        if line.startswith("#"):
            continue
        if not instruction_lines and not line:
            continue

        instruction_lines.append(line.removesuffix("\\").rstrip())
        if line.endswith("\\"):
            continue

        instruction = " ".join(instruction_lines)
        instruction_lines = []
        if not instruction.startswith("RUN "):
            continue

        lexer = shlex.shlex(instruction[4:], posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        lexer.commenters = "#"
        tokens = list(lexer)
        command: list[str] = []
        for token in tokens + [";"]:
            if token not in {"&&", ";", "||", "|", "&"}:
                command.append(token)
                continue
            if command[:2] == ["apk", "add"] and required in command[2:]:
                return True
            command = []
    return False


def check_repository(root: Path) -> list[str]:
    failures: list[str] = []
    dockerfile = (root / POLICY_FILES[0]).read_text(encoding="utf-8")
    mcp_dockerfile = (root / POLICY_FILES[1]).read_text(encoding="utf-8")

    mcp_base_images = _FROM.findall(mcp_dockerfile)
    if mcp_base_images != [MCP_BASE_IMAGE]:
        failures.append(
            "MCP image must use the reviewed Alpine base digest: "
            f"expected {MCP_BASE_IMAGE}, found {mcp_base_images}"
        )
    for package in ("libcrypto3", "libssl3"):
        required = f"{package}>={MCP_OPENSSL_MINIMUM}"
        if not _has_apk_add_argument(mcp_dockerfile, required):
            failures.append(
                "MCP image must enforce the reviewed OpenSSL package floor: "
                f"missing '{required}' from executable apk add arguments"
            )

    for name, contents in (("Dockerfile", dockerfile), ("mcp-server/Dockerfile", mcp_dockerfile)):
        for image in _FROM.findall(contents):
            if "@sha256:" not in image:
                failures.append(f"digest-pinned FROM requirement violated in {name}: {image}")

    if "RUN npm ci" not in dockerfile or "RUN npm install" in dockerfile:
        failures.append("npm production build must install from the lockfile with npm ci")

    return failures


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    failures = check_repository(root)
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1
    print("ECM and MCP container image input policy: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
