import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    select,
    func,
    text,
    Index,
    update,
    bindparam,
    PrimaryKeyConstraint,
)
from sqlalchemy.sql import and_
from pgvector.sqlalchemy import Vector, HALFVEC
from sqlalchemy.types import String, DateTime, Text

from src.utils.logging import logging
from src.helpers.enum import DBCOLUMNS
from src.utils.utils import has_table_decorator

logger = logging.getLogger(__name__)


class DBConnector:

    TABLE = "articles"
    VECTOR_DIM = 1024

    operator_map = {
        "eq": lambda column, value: column == value,
        "gt": lambda column, value: column > value,
        "lt": lambda column, value: column < value,
        "ge": lambda column, value: column >= value,
        "le": lambda column, value: column <= value,
        "in": lambda column, value: column.in_(value),
        "like": lambda column, value: func.lower(column).like(func.lower(value)),
        "notnull": lambda column, _: column.isnot(None),
        "isnull": lambda column, _: column.is_(None),
        "text_search": lambda column, value: text(
            f"{column.name} @@ to_tsquery('french', :query)"
        ).bindparams(query=value),
        "similarity": lambda column, value: text(
            f"{column.name} <#> :query < :threshold"
        ).bindparams(
            bindparam("query", value, type_=Vector(DBConnector.VECTOR_DIM)),
            bindparam("threshold", -0.5),
        ),
    }

    @staticmethod
    def get_engine():
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        db_url = f"postgresql://{user}:{password}@db:5432/{db_name}"
        assert None not in [
            user,
            password,
            db_name,
        ], "Failed to load the env variables"
        return create_engine(
            db_url,
            pool_size=11,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
        )

    @staticmethod
    def create_table(engine, table):
        metadata = MetaData()
        has_table = DBConnector.has_table(engine, table)

        if not has_table:
            logger.info(f"creating table {table}")
            table_ref = Table(
                table,
                metadata,
                Column(DBCOLUMNS.rowid.value, String, nullable=False),
                Column(DBCOLUMNS.date.value, DateTime, nullable=False),
                Column(DBCOLUMNS.archive.value, String, nullable=False),
                Column(DBCOLUMNS.image.value, Text, nullable=True),
                Column(DBCOLUMNS.title.value, String, nullable=True),
                Column(DBCOLUMNS.content.value, String, nullable=True),
                Column(DBCOLUMNS.tag.value, String, nullable=True),
                Column(DBCOLUMNS.link.value, String, nullable=False, unique=True),
                Column(
                    DBCOLUMNS.embedding.value,
                    HALFVEC(DBConnector.VECTOR_DIM),
                    nullable=False,
                ),
                PrimaryKeyConstraint(DBCOLUMNS.rowid.value),
            )

            metadata.create_all(engine)

            index_asc = Index(
                "rowid_date_asc_index",
                table_ref.c[DBCOLUMNS.rowid].asc(),
                table_ref.c[DBCOLUMNS.date].asc(),
            )
            index_asc.create(bind=engine)

            index_desc = Index(
                "rowid_date_desc_index",
                table_ref.c[DBCOLUMNS.rowid].desc(),
                table_ref.c[DBCOLUMNS.date].desc(),
            )
            index_desc.create(bind=engine)

            DBConnector.add_searchable_column(engine, table, DBCOLUMNS.text_searchable)
            DBConnector.add_vector_index(engine, table, DBCOLUMNS.embedding.value)

            return table_ref

        return Table(DBConnector.TABLE, metadata, autoload_with=engine)

    @staticmethod
    def has_table(engine, table):
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return table in metadata.tables

    @staticmethod
    def add_searchable_column(engine, table, column_name):
        with engine.connect() as connection:
            connection.execute(
                text(
                    f"ALTER TABLE {table} ADD COLUMN {column_name} tsvector "
                    "GENERATED ALWAYS AS "
                    "(to_tsvector('french', coalesce(title, '') "
                    "|| ' ' || coalesce(content, ''))) STORED"
                )
            )
            connection.execute(
                text(
                    f"CREATE INDEX {column_name}_index ON {table} "
                    f"USING GIN ({column_name})"
                )
            )
            connection.commit()

    @staticmethod
    def add_vector_index(engine, table, column_name):
        with engine.connect() as connection:
            connection.execute(
                text(
                    f"CREATE INDEX {column_name}_index ON {table} "
                    f"USING hnsw ({column_name} halfvec_cosine_ops)"
                )
            )
            connection.commit()

    @staticmethod
    def apply_filters(query, table_ref, filters):
        if filters:
            conditions = []
            for column, ops in filters.items():
                col = getattr(table_ref.c, column, None)
                assert (
                    col is not None
                ), f"Column '{column}' does not exist in the table."

                for op, value in ops:
                    assert (
                        op in DBConnector.operator_map
                    ), f"Operator '{op}' is not supported."
                    operator_func = DBConnector.operator_map.get(op)
                    conditions.append(operator_func(col, value))

                if conditions:
                    query = query.where(and_(*conditions))
        return query

    @has_table_decorator
    @staticmethod
    def get_total_count(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(func.count()).select_from(table_ref)

            query = DBConnector.apply_filters(query, table_ref, filters)

            total_count = connection.execute(query).scalar()

        return total_count

    @has_table_decorator
    @staticmethod
    def get_done_dates(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(table_ref.c[DBCOLUMNS.date])

            query = DBConnector.apply_filters(query, table_ref, filters)

            query = query.distinct()

            result = connection.execute(query)
            return [row[0] for row in result]

    @has_table_decorator
    @staticmethod
    def get_all_rowid(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(table_ref.c[DBCOLUMNS.rowid])
            query = DBConnector.apply_filters(query, table_ref, filters)
            result = connection.execute(query)
            return [row[0] for row in result]

    @has_table_decorator
    @staticmethod
    def get_all_rows(engine, table, filters=None, columns=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)
        columns = columns if columns else list(DBCOLUMNS)

        with engine.connect() as connection:
            query = select(*(table_ref.c[col] for col in columns))
            query = DBConnector.apply_filters(query, table_ref, filters)
            result = connection.execute(query)
            return result.fetchall()

    @has_table_decorator
    @staticmethod
    def get_archive_rows(engine, table, archive, columns=None, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)
        columns = columns if columns else list(DBCOLUMNS)

        with engine.connect() as connection:
            query = select(
                *(table_ref.c[col] for col in columns) if columns else table_ref
            ).where(table_ref.c.archive == archive)
            query = DBConnector.apply_filters(query, table_ref, filters)
            result = connection.execute(query)
            return result.fetchall()

    @has_table_decorator
    @staticmethod
    def get_archive_count(engine, table, archive, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(func.count()).where(table_ref.c.archive == archive)
            query = DBConnector.apply_filters(query, table_ref, filters)
            result = connection.execute(query)
            count = result.scalar()
            return count if count else 0

    @has_table_decorator
    @staticmethod
    def get_archive_freq(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(table_ref.c.archive, func.count())
            query = DBConnector.apply_filters(query, table_ref, filters)
            query = query.group_by(table_ref.c.archive).order_by(func.count().desc())
            result = connection.execute(query)
            return result.fetchall()

    @has_table_decorator
    @staticmethod
    def get_tags(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(
                func.trim(func.upper(table_ref.c[DBCOLUMNS.tag])).label("tag"),
                func.count().label("count"),
            )
            query = DBConnector.apply_filters(query, table_ref, filters)

            query = (
                query.group_by(func.trim(func.upper(table_ref.c[DBCOLUMNS.tag])))
                .order_by(func.count().desc())
                .limit(100)
            )
            result = connection.execute(query)

            return [(row.tag) for row in result]

    @has_table_decorator
    @staticmethod
    def fetch_data_keyset(
        engine,
        table,
        last_seen_value=None,
        direction="forward",
        columns=None,
        limit=10,
        filters=None,
        desc_order=True,
    ):

        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        if (direction == "forward" and desc_order) or (
            direction == "backward" and not desc_order
        ):
            order_by_clause = [
                table_ref.c[DBCOLUMNS.date].desc(),
                table_ref.c[DBCOLUMNS.rowid].desc(),
            ]
        else:
            order_by_clause = [
                table_ref.c[DBCOLUMNS.date].asc(),
                table_ref.c[DBCOLUMNS.rowid].asc(),
            ]

        query = select(*[table_ref.c[col] for col in columns] if columns else table_ref)

        query = DBConnector.apply_filters(query, table_ref, filters)

        if last_seen_value:
            if (direction == "forward" and desc_order) or (
                direction == "backward" and not desc_order
            ):
                query = query.where(
                    (table_ref.c[DBCOLUMNS.date] < last_seen_value[DBCOLUMNS.date])
                    | (
                        and_(
                            table_ref.c[DBCOLUMNS.date]
                            == last_seen_value[DBCOLUMNS.date],
                            table_ref.c[DBCOLUMNS.rowid]
                            < last_seen_value[DBCOLUMNS.rowid],
                        )
                    )
                )
            else:
                query = query.where(
                    (table_ref.c[DBCOLUMNS.date] > last_seen_value[DBCOLUMNS.date])
                    | (
                        and_(
                            table_ref.c[DBCOLUMNS.date]
                            == last_seen_value[DBCOLUMNS.date],
                            table_ref.c[DBCOLUMNS.rowid]
                            > last_seen_value[DBCOLUMNS.rowid],
                        )
                    )
                )

        if DBCOLUMNS.embedding in filters:
            embedding_vector = filters[DBCOLUMNS.embedding][0][1]
            query = query.order_by(
                text("embedding <#> :query").bindparams(
                    bindparam(
                        "query",
                        embedding_vector,
                        type_=Vector(DBConnector.VECTOR_DIM),
                    )
                )
            ).limit(limit)
        else:
            query = query.order_by(*order_by_clause).limit(limit)

        with engine.connect() as connection:
            result = connection.execute(query)

        fetched_data = result.fetchall()

        return fetched_data if direction == "forward" else fetched_data[::-1]

    @has_table_decorator
    @staticmethod
    def group_by(engine, table, value, filters=None):
        assert value in ["day", "month", "year"]
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(
                func.date_trunc(value, table_ref.c.date).label(value),
                func.count(table_ref.c.rowid).label("count"),
            )
            query = DBConnector.apply_filters(query, table_ref, filters)
            query = query.group_by(func.date_trunc(value, table_ref.c.date)).order_by(
                func.date_trunc(value, table_ref.c.date)
            )

            result = connection.execute(query)
            return result.fetchall()

    @has_table_decorator
    @staticmethod
    def get_min_max_dates(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(func.min(table_ref.c.date), func.max(table_ref.c.date))

            query = DBConnector.apply_filters(query, table_ref, filters)

            result = connection.execute(query)
            min_date, max_date = result.fetchone()

            return [min_date, max_date]

    @has_table_decorator
    @staticmethod
    def insert_row(engine, table, values):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        insert_stmt = table_ref.insert().values(values)

        if "postgresql" in str(engine.url):
            from sqlalchemy.dialects.postgresql import insert

            insert_stmt = insert(table_ref).values(values).on_conflict_do_nothing()

        with engine.connect() as connection:
            connection.execute(insert_stmt)
            connection.commit()
        logger.debug(f"inserted {len(values)} rows")

    @has_table_decorator
    @staticmethod
    def update_embedding(engine, table, values):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)
        keys = list(values[0].keys())
        stmt = (
            update(table_ref)
            .where(table_ref.c[DBCOLUMNS.rowid] == bindparam(keys[0]))
            .values({DBCOLUMNS.embedding: bindparam(keys[1])})
        )
        with engine.connect() as connection:
            connection.execute(stmt, values)
            connection.commit()

    @has_table_decorator
    @staticmethod
    def delete_row(engine, table, condition):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        delete_query = table_ref.delete().where(
            *[getattr(table_ref.c, key) == value for key, value in condition.items()]
        )

        with engine.connect() as connection:
            connection.execute(delete_query)
            connection.commit()

    @has_table_decorator
    @staticmethod
    def drop_table(engine, table):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        table_ref.drop(engine)

    @has_table_decorator
    @staticmethod
    def export_text_to_csv(engine, table, output_csv, columns):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)
        title_min_words = 5
        content_min_words = 10
        chunksize = 100000

        selected_cols = [table_ref.c[col] for col in columns]
        query = (
            table_ref.select()
            .with_only_columns(*selected_cols)
            .where(
                func.array_length(
                    func.string_to_array(table_ref.c[DBCOLUMNS.title], " "), 1
                )
                >= title_min_words
            )
            .where(
                func.array_length(
                    func.string_to_array(table_ref.c[DBCOLUMNS.content], " "), 1
                )
                >= content_min_words
            )
        )
        with engine.connect() as connection:
            with open(output_csv, "w", encoding="utf-8", newline="") as file:
                first_chunk = True
                for chunk in pd.read_sql(query, connection, chunksize=chunksize):
                    chunk.to_csv(file, index=False, header=first_chunk)
                    first_chunk = False
