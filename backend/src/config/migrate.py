"""Idempotent schema upgrade for existing databases.

``Base.metadata.create_all`` only creates missing tables; it never adds columns to
an existing ``simulation_runs`` table.  The reviewed resumable-run migration is
applied on startup so an existing named volume keeps its old completed runs and
gains the new progress/lease columns.  Fresh databases are created from metadata
first, so these statements are no-ops there.
"""
import logging
from pathlib import Path
from sqlalchemy import text
from .database import engine

MIGRATIONS=Path(__file__).resolve().parents[2]/"migrations"
MIGRATION_FILES=("002_resumable_runs.sql",)


def _statements(sql):
    lines=[line.split("--",1)[0] for line in sql.splitlines()]
    for chunk in "\n".join(lines).split(";"):
        statement=chunk.strip()
        if statement:yield statement


def migrate_schema():
    if engine.dialect.name!="postgresql":return
    with engine.begin() as connection:
        for name in MIGRATION_FILES:
            path=MIGRATIONS/name
            if not path.exists():continue
            for statement in _statements(path.read_text()):
                connection.execute(text(statement))
    logging.info("resumable-run schema verified")
