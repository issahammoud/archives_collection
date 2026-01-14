"""Initial migration - existing schema

Revision ID: 001
Revises:
Create Date: 2024-01-01 00:00:00.000000

NOTE: This migration represents the existing database schema.
The database was created using schema.sql and this migration
is for documentation purposes only. It should be marked as
applied without running the upgrade.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import HALFVEC


revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # The articles table already exists via schema.sql
    # This migration documents the existing schema
    #
    # CREATE TABLE articles (
    #     rowid BIGSERIAL PRIMARY KEY,
    #     date DATE NOT NULL,
    #     archive VARCHAR NOT NULL,
    #     image TEXT,
    #     title VARCHAR,
    #     content VARCHAR,
    #     tag VARCHAR,
    #     link VARCHAR NOT NULL,
    #     hash BIGINT GENERATED ALWAYS AS (hashtext(link)::BIGINT) STORED UNIQUE,
    #     embedding HALFVEC(1024),
    #     text_searchable TSVECTOR GENERATED ALWAYS AS (
    #         to_tsvector('french', unaccent_immutable(coalesce(title, ''))) ||
    #         to_tsvector('french', unaccent_immutable(coalesce(content, '')))
    #     ) STORED
    # );
    #
    # Indexes:
    # - articles_date_rowid_index ON articles (date, rowid)
    # - articles_text_searchable_index ON articles USING GIN (text_searchable)
    # - articles_embedding_index ON articles USING hnsw (embedding halfvec_ip_ops)
    #   WITH (m = 32, ef_construction = 128) WHERE embedding IS NOT NULL
    pass


def downgrade() -> None:
    # Do not drop the table - it contains production data
    pass
