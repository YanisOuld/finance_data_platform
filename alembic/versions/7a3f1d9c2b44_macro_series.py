"""macro_series

Revision ID: 7a3f1d9c2b44
Revises: 252ffb161d09
Create Date: 2026-06-30 16:55:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7a3f1d9c2b44"
down_revision: str | Sequence[str] | None = "252ffb161d09"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "macro_series",
        sa.Column("series", sa.String(), nullable=False),
        sa.Column("ts", sa.Date(), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("source", sa.String(), server_default=sa.text("'fred'"), nullable=False),
        sa.Column("run_id", sa.String(), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("series", "ts"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("macro_series")
