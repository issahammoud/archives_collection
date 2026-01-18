"""Initial schema without embedding column

Revision ID: 001_initial_schema
Revises:
Create Date: 2026-01-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create required extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
    op.execute('CREATE EXTENSION IF NOT EXISTS unaccent')

    # Create articles table
    op.create_table(
        'articles',
        sa.Column('rowid', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('archive', sa.String(), nullable=False),
        sa.Column('image', sa.Text(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('content', sa.String(), nullable=True),
        sa.Column('tag', sa.String(), nullable=True),
        sa.Column('link', sa.String(), nullable=False),
        sa.Column(
            'hash',
            sa.BigInteger(),
            sa.Computed("hashtext(link)::BIGINT", persisted=True),
            nullable=False,
        ),
        sa.Column(
            'text_searchable',
            postgresql.TSVECTOR(),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint('rowid'),
        sa.UniqueConstraint('hash', name='articles_hash_key'),
    )

    # Create indexes
    op.create_index(
        'articles_date_rowid_index',
        'articles',
        ['date', 'rowid'],
        unique=False,
    )

    op.create_index(
        'articles_text_searchable_index',
        'articles',
        ['text_searchable'],
        unique=False,
        postgresql_using='gin',
    )


def downgrade() -> None:
    op.drop_index('articles_text_searchable_index', table_name='articles')
    op.drop_index('articles_date_rowid_index', table_name='articles')
    op.drop_table('articles')
