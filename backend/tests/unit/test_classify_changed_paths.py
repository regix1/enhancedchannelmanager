"""Tests for standalone changed-path classification and image-only CI policy.

The classifier retains its fail-open path, parsing, verdict, and command-line
contracts even though current GitHub Actions only builds container images.
Inputs stay inline so those standalone contracts do not depend on repository
workflow churn.

The workflow checks read structured YAML from the real tree or explicit
temporary controls. They pin the current image-only envelope and reject any
shared-action or direct classifier consumer.
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "classify_changed_paths.py"


def _load_script_module():
    """Load classify_changed_paths.py as an ad-hoc module (it is not a package)."""
    spec = importlib.util.spec_from_file_location("classify_changed_paths", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["classify_changed_paths"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    return _load_script_module()


# --- The only paths CI may treat as inert machine state ---------------------


class TestInertPaths:
    @pytest.mark.parametrize(
        "path",
        [
            ".beads/enhancedchannelmanager.jsonl",
            ".beads/issues.jsonl.bak",
            ".beads/metadata.json",
        ],
    )
    def test_recognised_as_inert(self, script, path):
        assert script.is_inert_path(path) is True


class TestCodePaths:
    @pytest.mark.parametrize(
        "path",
        [
            "backend/main.py",
            "frontend/src/App.tsx",
            ".github/workflows/test.yml",
            "scripts/classify_changed_paths.py",
            "frontend/package.json",
            # A directory named like a doc file is still not a doc file: the
            # rule matches on the trailing path segment.
            "docs.md/handler.py",
            # `beads` without the leading dot is an ordinary directory.
            "beads/tool.py",
            # `**.md` matches the file NAME, so a bare `.md` file is not prose.
            ".md",
            ".beads/config.yaml",
            ".beads/.gitignore",
            ".beads/backup/2026-08-08.db",
            ".beads/config.json",
            ".beads/metadata.json.bak",
            ".beads/nested/issues.jsonl",
            ".beads/issues.jsonl.bak.extra",
            # Live Markdown fixture consumed by a backend test.
            "docs/style_guide.md",
            "docs/security/threat_model_dbas_import.md",
            "docs/shipping.md",
            "README.md",
            "CHANGELOG.md",
            "nested/notes.md",
            "docs/user_guide/backup-restore/run-a-restore-drill.md",
            "symlink-name.md",
            "docs/UPPER.MD",
            "docs/Mixed.Md",
            "docs/images/architecture.png",
            "graphify-out/memory/report.txt",
            "docs/prometheus_rules.yaml",
            "mkdocs.yml",
            "docs/requirements-docs.txt",
        ],
    )
    def test_recognised_as_code(self, script, path):
        assert script.is_inert_path(path) is False

    def test_beads_prefix_is_not_eaten_by_lstrip(self, script):
        """Regression guard: ``lstrip('./')`` would strip the leading dot of
        ``.beads/`` and turn every beads path into ``beads/``. The path must
        still classify as documentation."""
        assert script.is_inert_path(".beads/issues.jsonl") is True


class TestClassify:
    def test_root_machine_beads_state_only_is_inert(self, script):
        code_paths_changed, code_paths = script.classify(
            [".beads/issues.jsonl", ".beads/archive.jsonl.bak", ".beads/metadata.json"]
        )
        assert code_paths_changed is False
        assert code_paths == []

    def test_all_markdown_runs_code_gates(self, script):
        code_paths_changed, code_paths = script.classify(["CHANGELOG.md", "docs/api.md"])
        assert code_paths_changed is True
        assert code_paths == ["CHANGELOG.md", "docs/api.md"]

    def test_markdown_makes_machine_beads_change_code(self, script):
        code_paths_changed, code_paths = script.classify(
            [".beads/enhancedchannelmanager.jsonl", "docs/api.md"]
        )
        assert code_paths_changed is True
        assert code_paths == ["docs/api.md"]

    def test_mixed_change_is_code(self, script):
        """The defect this bead closes: a PR touching code AND Markdown.

        Every shipped change carries a CHANGELOG entry, so this is the shape
        of nearly every PR in the repo. It must classify as code."""
        code_paths_changed, code_paths = script.classify(["CHANGELOG.md", "backend/main.py"])
        assert code_paths_changed is True
        assert code_paths == ["CHANGELOG.md", "backend/main.py"]

    def test_code_only_is_code(self, script):
        code_paths_changed, code_paths = script.classify(["backend/main.py"])
        assert code_paths_changed is True
        assert code_paths == ["backend/main.py"]

    def test_empty_set_fails_open_to_code(self, script):
        """An undetermined file set must run the real work, never no-op green."""
        code_paths_changed, code_paths = script.classify([])
        assert code_paths_changed is True

    def test_blank_or_whitespace_names_fail_open_to_code(self, script):
        code_paths_changed, _ = script.classify(["", "  ", "docs/api.md"])
        assert code_paths_changed is True

    def test_blank_only_set_fails_open_to_code(self, script):
        code_paths_changed, _ = script.classify(["", "   ", "\t"])
        assert code_paths_changed is True


# --- Paths the published user-guide site is built from ----------------------


class TestDocsSitePaths:
    @pytest.mark.parametrize(
        "path",
        [
            "docs/user_guide/index.md",
            "docs/user_guide/stats/bandwidth.md",
            "docs/images/user_guide/dashboard.png",
            "docs/index.md",
            "mkdocs.yml",
            "docs/requirements-docs.txt",
            ".github/workflows/docs-pages.yml",
        ],
    )
    def test_recognised_as_site_input(self, script, path):
        assert script.is_docs_site_path(path) is True

    @pytest.mark.parametrize(
        "path",
        [
            # Internal documentation. `mkdocs.yml` excludes every one of
            # these directories, so changing them cannot change the site.
            "docs/shipping.md",
            "docs/testing.md",
            "docs/adr/ADR-005-code-security-gating-strategy.md",
            "docs/runbooks/restore.md",
            "docs/security/threat_model_dbas_import.md",
            "docs/sre/slos.md",
            # Images outside the published tree.
            "docs/images/architecture.png",
            # Source, and a workflow that is not the site's workflow.
            "backend/main.py",
            ".github/workflows/test.yml",
            # Prefix lookalikes: the match is on a path boundary.
            "docs/user_guides/index.md",
            "docs/index.markdown",
            "other/mkdocs.yml",
        ],
    )
    def test_recognised_as_not_site_input(self, script, path):
        assert script.is_docs_site_path(path) is False


class TestClassifyDocsSite:
    def test_a_user_guide_page_affects_the_site(self, script):
        affected, site_paths = script.classify_docs_site(
            ["docs/user_guide/stats/index.md", "backend/main.py"]
        )
        assert affected is True
        assert site_paths == ["docs/user_guide/stats/index.md"]

    def test_internal_docs_do_not_affect_the_site(self, script):
        affected, site_paths = script.classify_docs_site(
            ["docs/shipping.md", "docs/adr/ADR-001.md"]
        )
        assert affected is False
        assert site_paths == []

    def test_empty_set_fails_open_to_affected(self, script):
        """The opposite fail-open direction from ``code_paths_changed``.

        A missed rebuild leaves the published site stale behind merged
        content; a needless rebuild costs a runner minute."""
        affected, site_paths = script.classify_docs_site([])
        assert affected is True
        assert site_paths == []

    def test_blank_only_set_fails_open_to_affected(self, script):
        affected, _ = script.classify_docs_site(["", "   ", "\t"])
        assert affected is True

    def test_the_two_verdicts_are_independent(self, script):
        """Code-gate and documentation-site decisions remain independent.

        The four combinations are all reachable, so neither output may be
        derived from the other."""
        combinations = {
            ("docs/user_guide/index.md",): (True, True),
            ("docs/testing.md",): (True, False),
            ("mkdocs.yml",): (True, True),
            ("backend/main.py",): (True, False),
            (".beads/issues.jsonl",): (False, False),
        }
        for paths, expected in combinations.items():
            code_paths_changed, _ = script.classify(list(paths))
            affected, _ = script.classify_docs_site(list(paths))
            assert (code_paths_changed, affected) == expected, paths


# --- End-to-end CLI contract ------------------------------------------------


def _run_cli(args, stdin_text=""):
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), *args],
        input=stdin_text,
        capture_output=True,
        text=True,
        check=False,
    )


def _outputs(result) -> dict[str, str]:
    """Parse the ``key=value`` lines the workflow appends to $GITHUB_OUTPUT.

    Read by key, never by line position: the workflows do the same, so a
    future third output must not be able to break an existing consumer."""
    parsed = {}
    stdout = result.stdout.decode("utf-8") if isinstance(result.stdout, bytes) else result.stdout
    for line in stdout.splitlines():
        key, _, value = line.partition("=")
        parsed[key] = value
    return parsed


class TestCommandLine:
    def test_stdin_code_paths_changed(self):
        result = _run_cli([], json.dumps(["docs/api.md", "CHANGELOG.md"]))
        assert result.returncode == 0
        assert _outputs(result)["code_paths_changed"] == "true"

    def test_stdin_mixed(self):
        result = _run_cli([], json.dumps(["docs/api.md", "backend/main.py"]))
        assert result.returncode == 0
        assert _outputs(result)["code_paths_changed"] == "true"
        assert "backend/main.py" in result.stderr

    def test_files_from(self, tmp_path):
        listing = tmp_path / "changed.txt"
        listing.write_text(json.dumps(["docs/api.md", ".beads/x.jsonl"]), encoding="utf-8")
        result = _run_cli(["--files-from", str(listing)])
        assert result.returncode == 0
        assert _outputs(result)["code_paths_changed"] == "true"

    def test_missing_files_from_exits_zero_and_fails_open(self, tmp_path):
        """A classifier hiccup must never skip a dependent job."""
        result = _run_cli(["--files-from", str(tmp_path / "nope.txt")])
        assert result.returncode == 0
        assert _outputs(result) == {"code_paths_changed": "true", "docs_site_affected": "true"}
        assert "::warning::" in result.stderr

    def test_empty_input_exits_zero_and_fails_open(self):
        result = _run_cli([], "")
        assert result.returncode == 0
        assert _outputs(result) == {"code_paths_changed": "true", "docs_site_affected": "true"}
        assert "::warning::" in result.stderr

    def test_output_is_exactly_the_two_github_output_keys(self):
        """The workflow appends stdout straight to $GITHUB_OUTPUT, so every
        stdout line must be a well-formed key=value pair and nothing else.

        Pinned as a set, not a sequence: consumers read by key, and pinning
        the order would make adding an output a breaking change for no
        reason."""
        result = _run_cli([], json.dumps(["backend/main.py"]))
        assert set(_outputs(result)) == {"code_paths_changed", "docs_site_affected"}
        assert all("=" in line for line in result.stdout.splitlines())

    def test_site_verdict_on_a_published_page(self):
        result = _run_cli([], json.dumps(["docs/user_guide/stats/bandwidth.md"]))
        assert _outputs(result) == {"code_paths_changed": "true", "docs_site_affected": "true"}

    def test_site_verdict_on_an_internal_doc(self):
        result = _run_cli([], json.dumps(["docs/testing.md"]))
        assert _outputs(result) == {"code_paths_changed": "true", "docs_site_affected": "false"}

    @pytest.mark.parametrize(
        "path",
        ["docs/line\nbreak.md", r"docs\shipping.md", " docs/shipping.md", "docs/shipping.md "],
    )
    def test_lossless_odd_names_fail_open_to_code(self, path):
        result = _run_cli([], json.dumps([path]))
        assert _outputs(result)["code_paths_changed"] == "true"

    @pytest.mark.parametrize("payload", ["not json", "{}", '["docs/a.md", 1]', '["a\\u0000b"]'])
    def test_malformed_or_ambiguous_payload_fails_open(self, payload):
        result = _run_cli([], payload)
        assert _outputs(result) == {"code_paths_changed": "true", "docs_site_affected": "true"}

    def test_git_z_transport_preserves_rename_source_destination_and_delete(self):
        raw = "backend/source.py\0docs/destination.md\0backend/deleted.py\0"
        result = _run_cli(["--input-format", "nul"], raw)
        assert _outputs(result)["code_paths_changed"] == "true"

    def test_complete_envelope_classifies_known_paths(self):
        payload = {"complete": True, "paths": [".beads/state.jsonl"]}
        result = _run_cli(["--input-format", "envelope"], json.dumps(payload))
        assert _outputs(result) == {
            "code_paths_changed": "false",
            "docs_site_affected": "false",
        }

    @pytest.mark.parametrize(
        "payload",
        (
            {"complete": False, "paths": [".beads/state.jsonl"]},
            {"paths": [".beads/state.jsonl"]},
            {"complete": "true", "paths": [".beads/state.jsonl"]},
            {"complete": True},
        ),
    )
    def test_incomplete_or_malformed_envelope_fails_safe(self, payload):
        result = _run_cli(["--input-format", "envelope"], json.dumps(payload))
        assert _outputs(result) == {
            "code_paths_changed": "true",
            "docs_site_affected": "true",
        }
        assert "undetermined" in result.stderr.lower()
        assert ".beads/state.jsonl" not in result.stderr

    @pytest.mark.parametrize(
        "raw",
        [
            b"docs/line\nbreak.md\0",
            b"docs\\shipping.md\0",
            b" docs/shipping.md\0",
            b"docs/shipping.md \0",
            b"docs/shipping.md",  # missing terminal NUL is ambiguous
            b"docs/shipping.md\0\0",
        ],
    )
    def test_git_z_odd_or_malformed_names_fail_open(self, raw):
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--input-format", "nul"],
            input=raw,
            capture_output=True,
            check=False,
        )
        assert _outputs(result)["code_paths_changed"] == "true"


# --- The rule matches what the workflows gate on ----------------------------


class TestWorkflowContract:
    """Pin the current image-only workflow and classifier absence."""

    WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

    @staticmethod
    def _workflow_files() -> list[Path]:
        """Return every workflow file, using both accepted extensions."""
        directory = TestWorkflowContract.WORKFLOW_DIR
        return sorted(
            list(directory.glob("*.yml")) + list(directory.glob("*.yaml"))
        )

    @staticmethod
    def _load_workflow(path: Path) -> dict:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8"))

    def test_no_code_paths_changed_sentinel_workflow_remains(self):
        assert not (self.WORKFLOW_DIR / "docs-only-pass.yml").exists()

    def test_image_only_workflow_policy(self):
        workflows = self._workflow_files()
        assert [path.name for path in workflows] == ["build.yml"]

        workflow = self._load_workflow(workflows[0])
        assert set((workflow.get("jobs") or {}).keys()) == {
            "build-amd64",
            "build-arm64",
            "merge-manifests",
            "build-mcp-amd64",
            "build-mcp-arm64",
            "merge-mcp-manifests",
        }
        assert not ACTION_FILE.exists()
        _workflow_action_contract()


ACTION_USE = "./.github/actions/classify-changed-paths"
ACTION_FILE = REPO_ROOT / ".github/actions/classify-changed-paths/action.yml"


def _discover_action_consumers(
    directory: Path | None = None,
) -> tuple[tuple[Path, str, int], ...]:
    """Locate the exact local action structurally across every workflow job."""
    if directory is None:
        directory = TestWorkflowContract.WORKFLOW_DIR
    found = []
    for path in sorted((*directory.glob("*.yml"), *directory.glob("*.yaml"))):
        jobs = TestWorkflowContract._load_workflow(path).get("jobs") or {}
        for job_id, job in jobs.items():
            for index, step in enumerate((job or {}).get("steps") or []):
                if (step or {}).get("uses") == ACTION_USE:
                    found.append((path, job_id, index))
    return tuple(found)


def _workflow_action_contract(directory: Path | None = None) -> None:
    if directory is None:
        directory = TestWorkflowContract.WORKFLOW_DIR

    consumers = _discover_action_consumers(directory)
    assert not consumers, f"shared changed-path action found at {consumers}"

    for path in sorted((*directory.glob("*.yml"), *directory.glob("*.yaml"))):
        jobs = TestWorkflowContract._load_workflow(path).get("jobs") or {}
        for job_id, job in jobs.items():
            for index, step in enumerate((job or {}).get("steps") or []):
                run = (step or {}).get("run")
                assert not (
                    isinstance(run, str) and SCRIPT_PATH.stem in run
                ), f"{path.name}:{job_id}:{index}: direct classifier invocation"


@pytest.mark.parametrize("extension", [".yml", ".yaml"])
def test_classifier_workflow_discovery_includes_yaml(tmp_path, extension):
    workflow = tmp_path / f"build{extension}"
    workflow.write_text(
        f"""jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: {ACTION_USE}
""",
        encoding="utf-8",
    )

    assert _discover_action_consumers(tmp_path) == ((workflow, "build", 0),)
    with pytest.raises(AssertionError, match="shared changed-path action"):
        _workflow_action_contract(tmp_path)


@pytest.mark.parametrize(
    ("name", "command"),
    [
        ("build.yml", "python scripts/classify_changed_paths.py"),
        ("build.yaml", "python3 -m scripts.classify_changed_paths"),
    ],
)
def test_direct_classifier_invocation_is_rejected(tmp_path, name, command):
    workflow = tmp_path / name
    workflow.write_text(
        f"""jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: {command}
""",
        encoding="utf-8",
    )

    assert _discover_action_consumers(tmp_path) == ()
    with pytest.raises(AssertionError, match="direct classifier invocation"):
        _workflow_action_contract(tmp_path)


def test_yaml_comment_prose_is_ignored(tmp_path):
    workflow = tmp_path / "build.yml"
    workflow.write_text(
        f"""# {ACTION_USE}
# {SCRIPT_PATH.stem}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo image
""",
        encoding="utf-8",
    )

    parsed = TestWorkflowContract._load_workflow(workflow)
    assert parsed["jobs"]["build"] == {
        "runs-on": "ubuntu-latest",
        "steps": [{"run": "echo image"}],
    }
    assert _discover_action_consumers(tmp_path) == ()
    _workflow_action_contract(tmp_path)


def _load_yaml_ignoring_unknown_tags(path: Path) -> dict:
    """Parse YAML that carries tags SafeLoader refuses, such as mkdocs.yml.

    `mkdocs.yml` uses `!!python/name:` to wire the emoji extension. Resolving
    that tag would import the module; the tests only need the document
    structure, so unknown tags collapse to None."""
    import yaml

    class _Loader(yaml.SafeLoader):
        pass

    _Loader.add_multi_constructor("", lambda loader, suffix, node: None)
    return yaml.load(path.read_text(encoding="utf-8"), Loader=_Loader)


class TestDocsSiteWorkflowContract:
    """Keep published navigation within the classifier's recognized paths."""

    MKDOCS_CONFIG = REPO_ROOT / "mkdocs.yml"

    def test_every_published_nav_target_is_a_recognised_site_path(self, script):
        """Every Markdown target in the MkDocs navigation must be recognized."""
        config = _load_yaml_ignoring_unknown_tags(self.MKDOCS_CONFIG)
        docs_dir = config.get("docs_dir", "docs").strip("/")

        targets: list[str] = []

        def collect(node) -> None:
            if isinstance(node, str):
                if node.endswith(".md"):
                    targets.append(node)
            elif isinstance(node, list):
                for item in node:
                    collect(item)
            elif isinstance(node, dict):
                for value in node.values():
                    collect(value)

        collect(config.get("nav") or [])
        assert targets, "mkdocs.yml has no nav entries; the parse went wrong."

        missed = [
            target
            for target in targets
            if not script.is_docs_site_path(f"{docs_dir}/{target}")
        ]
        assert not missed, (
            f"{len(missed)} page(s) in the MkDocs navigation are outside the "
            f"path list in scripts/classify_changed_paths.py: {missed[:5]}."
        )
