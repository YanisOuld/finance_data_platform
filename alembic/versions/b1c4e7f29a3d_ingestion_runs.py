"""ingestion_runs

Revision ID: b1c4e7f29a3d
Revises: 7a3f1d9c2b44
Create Date: 2026-06-30 18:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b1c4e7f29a3d"
down_revision: str | Sequence[str] | None = "7a3f1d9c2b44"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "ingestion_runs",
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("dataset", sa.String(), nullable=False),
        sa.Column("run_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(), server_default=sa.text("'running'"), nullable=False),
        sa.Column("items_total", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("items_success", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("items_failed", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index("ix_ingestion_runs_dataset_run_date", "ingestion_runs", ["dataset", "run_date"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_ingestion_runs_dataset_run_date", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")
