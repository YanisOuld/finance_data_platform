"""instrument_figi

Revision ID: f7c2b8e4a1d3
Revises: d4a6c1f7e2b9
Create Date: 2026-06-30 23:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f7c2b8e4a1d3"
down_revision: str | Sequence[str] | None = "d4a6c1f7e2b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "instrument_figi",
        sa.Column("ticker", sa.String(), nullable=False),
        sa.Column("figi", sa.String(), nullable=False),
        sa.Column("composite_figi", sa.String(), nullable=True),
        sa.Column("share_class_figi", sa.String(), nullable=True),
        sa.Column("security_type", sa.String(), nullable=True),
        sa.Column("market_sector", sa.String(), nullable=True),
        sa.Column("exch_code", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("source", sa.String(), server_default=sa.text("'openfigi'"), nullable=False),
        sa.Column("run_id", sa.String(), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["ticker"], ["universal_instruments.ticker"]),
        sa.PrimaryKeyConstraint("ticker", "figi"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("instrument_figi")
