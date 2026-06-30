"""fundamentals

Revision ID: d4a6c1f7e2b9
Revises: b1c4e7f29a3d
Create Date: 2026-06-30 17:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4a6c1f7e2b9"
down_revision: str | Sequence[str] | None = "b1c4e7f29a3d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "fundamentals",
        sa.Column("ticker", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("fp", sa.String(), nullable=False),
        sa.Column("form", sa.String(), nullable=False),
        sa.Column("fy", sa.BigInteger(), nullable=True),
        sa.Column("period_start", sa.Date(), nullable=True),
        sa.Column("val", sa.Float(), nullable=False),
        sa.Column("accn", sa.String(), nullable=True),
        sa.Column("filed", sa.Date(), nullable=True),
        sa.Column("source", sa.String(), server_default=sa.text("'sec_edgar'"), nullable=False),
        sa.Column("run_id", sa.String(), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("ticker", "concept", "unit", "period_end", "fp", "form"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("fundamentals")
