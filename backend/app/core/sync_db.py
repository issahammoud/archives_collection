"""Synchronous database manager for Celery tasks and data scraping."""

import logging
from sqlalchemy import create_engine, MetaData, Table, func, select, inspect
from sqlalchemy.dialects.postgresql import insert

from app.core.config import settings

logger = logging.getLogger(__name__)


class SyncDBManager:
    _engine = None

    @classmethod
    def get_engine(cls):
        if cls._engine is None:
            cls._engine = create_engine(
                settings.sync_database_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,
            )
        return cls._engine

    @classmethod
    def get_total_count(cls, table_name, filters=None):
        engine = cls.get_engine()
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return 0

        metadata = MetaData()
        table_ref = Table(table_name, metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = select(func.count()).select_from(table_ref)
            if filters:
                for col_name, ops in filters.items():
                    for op, val in ops:
                        col = table_ref.c[col_name]
                        if op == "eq":
                            query = query.where(col == val)
            result = conn.execute(query)
            return result.scalar() or 0

    @classmethod
    def insert_rows(cls, table_name, values):
        engine = cls.get_engine()
        metadata = MetaData()
        table_ref = Table(table_name, metadata, autoload_with=engine)

        with engine.connect() as conn:
            with conn.begin():
                stmt = insert(table_ref).values(values).on_conflict_do_nothing()
                result = conn.execute(stmt)
                return result.rowcount
