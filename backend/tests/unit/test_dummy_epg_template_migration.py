"""The stored dummy EPG tvg-id template is repointed onto the channel id.

A profile row written before the code default flipped still holds
``ecm-{channel_number}``, and a SQLAlchemy ``default=`` never revisits an
existing row.

The fix lives in ``_run_migrations`` rather than in an Alembic revision, and
these tests are built around the reason why. ``_bootstrap_alembic`` stamps
``alembic_version`` forward without running ``upgrade head`` whenever the live
schema already covers the model shape, which is the state of every install
carrying the old value, because ``tvg_id_template`` has existed since the
baseline. So the setup here reproduces that exact install: full head schema,
``alembic_version`` left behind, and the old literal in the table.

All fixtures use synthetic ids — no production-derived data.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text

import database


OLD_TEMPLATE = "ecm-{channel_number}"
NEW_TEMPLATE = "ecm-{channel_id}"
CUSTOM_TEMPLATE = "ecmlive-{channel_id}-hd"
LAGGING_REVISION = "0035"


def _make_alembic_config(db_url: str):
    """Build an Alembic Config pinned to *db_url* (self-contained per convention)."""
    from alembic.config import Config

    ini_path = Path(database.ALEMBIC_INI_PATH)
    assert ini_path.exists(), f"alembic.ini missing at {ini_path}"
    cfg = Config(str(ini_path))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_profile(engine, profile_id: int, name: str, template: str) -> None:
    """Insert a dummy EPG profile carrying *template* as its stored tvg-id template."""
    now = datetime.utcnow().isoformat()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dummy_epg_profiles "
            "(id, name, enabled, name_source, stream_index, event_timezone, "
            " program_duration, tvg_id_template, include_date_tag, "
            " include_live_tag, include_new_tag, created_at, updated_at) "
            "VALUES (:id, :name, 1, 'channel', 0, 'UTC', 240, :template, "
            " 0, 1, 0, :now, :now)"
        ), {"id": profile_id, "name": name, "template": template, "now": now})


def _templates(engine) -> dict:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, tvg_id_template FROM dummy_epg_profiles ORDER BY id"
        )).fetchall()
    return {row[0]: row[1] for row in rows}


def _stamped_revision(engine) -> str:
    with engine.connect() as conn:
        return conn.execute(text("SELECT version_num FROM alembic_version")).scalar()


def _make_fast_path_install(tmp_path, db_name: str):
    """Build the install that trips the stamp-forward fast path.

    Full head schema, three profiles, and ``alembic_version`` rewound so it lags
    head. That combination is what makes ``_bootstrap_alembic`` stamp forward
    instead of running ``upgrade head``.
    """
    from alembic import command

    db_url = f"sqlite:///{tmp_path / db_name}"
    command.upgrade(_make_alembic_config(db_url), "head")

    engine = create_engine(db_url, future=True)
    _insert_profile(engine, 1, "Upgraded install", OLD_TEMPLATE)
    _insert_profile(engine, 2, "Fresh install", NEW_TEMPLATE)
    _insert_profile(engine, 3, "Operator edited", CUSTOM_TEMPLATE)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE alembic_version SET version_num = :rev"),
            {"rev": LAGGING_REVISION},
        )
    return engine


class TestDummyEPGTemplateMigration:
    """The rewrite has to survive the startup path that skips Alembic."""

    def test_the_fast_path_stamps_forward_and_runs_no_revision(self, tmp_path):
        """Pin the state the fix has to work through.

        This is the discriminator for the test below: it asserts the fixture is
        healthy and that Alembic really did skip. If the setup ever stopped
        producing a fast-path install, this test fails first and names that,
        instead of the rewrite test failing for a reason that looks like a
        missing fixup.
        """
        engine = _make_fast_path_install(tmp_path, "fastpath_probe.db")
        try:
            database._bootstrap_alembic(engine)

            assert _stamped_revision(engine) == database.get_alembic_head_revision(), (
                "expected the fast path to stamp forward to head"
            )
            assert _templates(engine)[1] == OLD_TEMPLATE, (
                "a revision ran after all — the fast path did not fire"
            )
        finally:
            engine.dispose()

    def test_the_rewrite_happens_on_a_fast_path_install(self, tmp_path):
        """The startup fixup rewrites the old literal even though Alembic skipped."""
        engine = _make_fast_path_install(tmp_path, "fastpath_rewrite.db")
        try:
            database._bootstrap_alembic(engine)
            database._run_migrations(engine)

            assert _templates(engine)[1] == NEW_TEMPLATE
        finally:
            engine.dispose()

    def test_a_customised_template_is_left_alone(self, tmp_path):
        """Only the exact old literal is rewritten; anything else survives."""
        engine = _make_fast_path_install(tmp_path, "fastpath_custom.db")
        try:
            database._run_migrations(engine)

            stored = _templates(engine)
            assert stored[3] == CUSTOM_TEMPLATE, "an operator's template was rewritten"
            assert stored[2] == NEW_TEMPLATE, "an already-correct template was disturbed"
        finally:
            engine.dispose()

    def test_a_second_startup_changes_nothing(self, tmp_path):
        """Re-running is a no-op — the exact-literal predicate is the gate."""
        engine = _make_fast_path_install(tmp_path, "fastpath_twice.db")
        try:
            database._run_migrations(engine)
            after_first = _templates(engine)

            database._run_migrations(engine)

            assert _templates(engine) == after_first
            assert after_first == {
                1: NEW_TEMPLATE,
                2: NEW_TEMPLATE,
                3: CUSTOM_TEMPLATE,
            }
        finally:
            engine.dispose()
