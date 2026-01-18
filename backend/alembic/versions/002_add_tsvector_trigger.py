"""Add tsvector trigger for text search

Revision ID: 002_add_tsvector_trigger
Revises: 001_initial_schema
Create Date: 2026-01-18

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '002_add_tsvector_trigger'
down_revision: Union[str, None] = '001_initial_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop trigger if it exists (for idempotency)
    op.execute('DROP TRIGGER IF EXISTS articles_text_searchable_update ON articles')

    # Create function to generate tsvector from title and content
    op.execute("""
        CREATE OR REPLACE FUNCTION articles_text_searchable_trigger()
        RETURNS trigger AS $$
        BEGIN
            NEW.text_searchable :=
                setweight(to_tsvector('french', unaccent(COALESCE(NEW.title, ''))), 'A') ||
                setweight(to_tsvector('french', unaccent(COALESCE(NEW.content, ''))), 'B');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """)

    # Create trigger to automatically update text_searchable on INSERT or UPDATE
    op.execute("""
        CREATE TRIGGER articles_text_searchable_update
        BEFORE INSERT OR UPDATE OF title, content
        ON articles
        FOR EACH ROW
        EXECUTE FUNCTION articles_text_searchable_trigger();
    """)

    # Update existing rows to populate text_searchable
    op.execute("""
        UPDATE articles
        SET text_searchable =
            setweight(to_tsvector('french', unaccent(COALESCE(title, ''))), 'A') ||
            setweight(to_tsvector('french', unaccent(COALESCE(content, ''))), 'B')
        WHERE text_searchable IS NULL;
    """)


def downgrade() -> None:
    op.execute('DROP TRIGGER IF EXISTS articles_text_searchable_update ON articles')
    op.execute('DROP FUNCTION IF EXISTS articles_text_searchable_trigger()')
