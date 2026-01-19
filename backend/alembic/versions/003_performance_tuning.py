"""Optimize Image, Date Grouping, and Archive counts

Revision ID: 003_performance_tuning
Revises: 002_add_tsvector_trigger
Create Date: 2026-01-19
"""
from alembic import op
import sqlalchemy as sa

revision = '003_performance_tuning'
down_revision = '002_add_tsvector_trigger'

def upgrade() -> None:
    # 1. Fix for get_total_count (Image Filter)
    # Target: 512ms -> <50ms
    op.create_index(
        'articles_with_images_idx',
        'articles',
        [sa.text('date DESC')],
        postgresql_where=sa.text("image IS NOT NULL AND image != ''")
    )

    # 2. Fix for get_archive_counts and general Date sorting
    # Target: Speeds up GROUP BY archive
    op.create_index(
        'articles_archive_date_idx',
        'articles',
        ['archive', 'date']
    )

    # 3. Fix for group_by (Day/Month/Year)
    # Even with date_trunc, having a narrow index on just 'date' 
    # allows for an Index Only Scan.
    op.create_index(
        'articles_date_simple_idx',
        'articles',
        ['date']
    )

def downgrade() -> None:
    op.drop_index('articles_date_simple_idx', table_name='articles')
    op.drop_index('articles_archive_date_idx', table_name='articles')
    op.drop_index('articles_with_images_idx', table_name='articles')