"""Add article_embeddings table with pgvector halfvec

Revision ID: 004_add_article_embeddings
Revises: 003_performance_tuning
Create Date: 2026-01-24
"""
from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import HALFVEC

revision = '004_add_article_embeddings'
down_revision = '003_performance_tuning'

EMBEDDING_DIM = 256


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # Create the article_embeddings table with halfvec (16-bit) for space efficiency
    op.create_table(
        'article_embeddings',
        sa.Column('rowid', sa.BigInteger, sa.ForeignKey('articles.rowid', ondelete='CASCADE'), primary_key=True),
        sa.Column('embedding', HALFVEC(EMBEDDING_DIM), nullable=True),
    )

    # Create HNSW index for fast similarity search
    # Using inner product (ip) since Jina embeddings are normalized
    # For normalized vectors: cosine_similarity = inner_product
    op.execute("""
        CREATE INDEX article_embeddings_hnsw_idx
        ON article_embeddings
        USING hnsw (embedding halfvec_ip_ops)
        WITH (m = 16, ef_construction = 64)
    """)


def downgrade() -> None:
    op.drop_index('article_embeddings_hnsw_idx', table_name='article_embeddings')
    op.drop_table('article_embeddings')
