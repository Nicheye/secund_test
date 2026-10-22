"""track failed outbox publishes

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # A constant server default is a metadata-only change on PostgreSQL 11+, so existing rows
    # get 0 without a table rewrite.
    op.add_column(
        "outbox",
        sa.Column("publish_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column("outbox", sa.Column("last_error", sa.Text(), nullable=True))
    op.drop_index("ix_outbox_unpublished_created_at", table_name="outbox")
    op.create_index(
        "ix_outbox_unpublished_order",
        "outbox",
        ["publish_attempts", "created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished_order", table_name="outbox")
    op.create_index(
        "ix_outbox_unpublished_created_at",
        "outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_column("outbox", "last_error")
    op.drop_column("outbox", "publish_attempts")
