"""status_events: monotonic seq for deterministic history ordering

Adds a database-generated identity column ``seq`` to ``status_events`` so the
per-notification history has a strict total order, independent of the
transaction-timestamp ``created_at`` (which can tie across rapid transitions or a
batch's creation events). Postgres backfills existing rows from the identity
sequence when the column is added.

Revision ID: 0002_status_event_seq
Revises: 0001_initial
Create Date: 2026-06-09

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_status_event_seq"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "status_events",
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=False), nullable=False),
    )
    op.create_index("ix_status_events_seq", "status_events", ["seq"])


def downgrade() -> None:
    op.drop_index("ix_status_events_seq", table_name="status_events")
    op.drop_column("status_events", "seq")
