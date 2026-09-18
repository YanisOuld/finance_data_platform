"""api_key_scopes

Revision ID: e1f2a3b4c5d6
Revises: a1b2c3d4e5f6
Create Date: 2026-09-17 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: str | Sequence[str] | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "api_keys",
        sa.Column("scopes", sa.String(), server_default=sa.text("'read'"), nullable=False),
    )
    # Keys created before scoping existed had full read+write access; preserve
    # it so this migration never silently strips capability from a live key.
    # New keys created from here default to 'read'.
    op.execute("UPDATE api_keys SET scopes = 'read,write'")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("api_keys", "scopes")
