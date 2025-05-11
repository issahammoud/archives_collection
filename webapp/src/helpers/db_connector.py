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
    Computed,
    bindparam,
    BigInteger,
    true,
    tuple_,
    literal,
)
from sqlalchemy.sql import and_
from pgvector.sqlalchemy import Vector, HALFVEC
from sqlalchemy.types import String, Date, Text
from sqlalchemy.dialects.postgresql import insert

from src.utils.logging import logging
from src.helpers.enum import DBCOLUMNS, OPERATORS
from src.utils.utils import execute

logger = logging.getLogger(__name__)


class DBConnector:

    TABLE = "articles"
    VECTOR_DIM = 1024
    TOP_K = 1000
    THRESHOLD = -0.5

    operator_map = {
        OPERATORS.eq: lambda column, value: column == value,
        OPERATORS.gt: lambda column, value: column > value,
        OPERATORS.lt: lambda column, value: column < value,
        OPERATORS.ge: lambda column, value: column >= value,
        OPERATORS.le: lambda column, value: column <= value,
        OPERATORS.in_: lambda column, value: column.in_(value),
        OPERATORS.like: lambda column, value: func.lower(column).like(
            func.lower(value)
        ),
        OPERATORS.notnull: lambda column, _: column.isnot(None),
        OPERATORS.isnull: lambda column, _: column.is_(None),
        OPERATORS.ts: lambda column, value: text(
            f"{column.name} @@ to_tsquery('french', :query)"
        ).bindparams(query=value),
        OPERATORS.vs: lambda column, value: column.op("<#>")(
            bindparam("query", value, type_=Vector(DBConnector.VECTOR_DIM))
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

        engine = create_engine(
            db_url,
            pool_size=11,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
        )
        return engine

    @staticmethod
    def drop_table(engine, table):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        table_ref.drop(engine)

    @staticmethod
    def create_table(engine, table):
        metadata = MetaData()
        has_table = DBConnector.has_table(engine, table)

        if not has_table:
            logger.info(f"creating table {table}")
            table_ref = Table(
                table,
                metadata,
                Column(
                    DBCOLUMNS.rowid.value,
                    BigInteger,
                    primary_key=True,
                    autoincrement=True,
                ),
                Column(DBCOLUMNS.date.value, Date, nullable=False),
                Column(DBCOLUMNS.archive.value, String, nullable=False),
                Column(DBCOLUMNS.image.value, Text, nullable=True),
                Column(DBCOLUMNS.title.value, String, nullable=True),
                Column(DBCOLUMNS.content.value, String, nullable=True),
                Column(DBCOLUMNS.tag.value, String, nullable=True),
                Column(DBCOLUMNS.link.value, String, nullable=False),
                Column(
                    DBCOLUMNS.hash.value,
                    BigInteger,
                    Computed(
                        f"hashtext({DBCOLUMNS.link.value})::BIGINT", persisted=True
                    ),
                    nullable=False,
                    unique=True,
                ),
                Column(
                    DBCOLUMNS.embedding.value,
                    HALFVEC(DBConnector.VECTOR_DIM),
                    nullable=True,
                ),
                Index(
                    f"{DBConnector.TABLE}_date_rowid_index",
                    Column(DBCOLUMNS.date.value),
                    Column(DBCOLUMNS.rowid.value),
                ),
            )

            metadata.create_all(engine)

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
                    f"CREATE INDEX {DBConnector.TABLE}_{column_name}_index ON {table} "
                    f"USING GIN ({column_name})"
                )
            )
            connection.commit()

    @staticmethod
    def add_vector_index(engine, table, column_name):
        with engine.connect() as connection:
            connection.execute(
                text(
                    f"CREATE INDEX {DBConnector.TABLE}_{column_name}_index ON {table} "
                    f"USING hnsw ({column_name} halfvec_ip_ops) "
                    "WITH (m = 32, ef_construction = 128) "
                    f"WHERE {column_name} IS NOT NULL;"
                )
            )
            connection.commit()

    @staticmethod
    def apply_filters(query, table_ref, filters):
        if not filters:
            return query

        filters = dict(filters)
        sim_key = DBCOLUMNS.embedding
        base = table_ref
        orig_base = table_ref

        if sim_key in filters:
            ops = filters.pop(sim_key)
            for op, vec in ops:
                if op == OPERATORS.vs:
                    sim_expr = DBConnector.operator_map[op](base.c[sim_key], vec)
                    notnull = DBConnector.operator_map[OPERATORS.notnull](
                        base.c[sim_key], None
                    )
                    subq = (
                        select(*base.c, sim_expr.label("similarity"))
                        .where(and_(sim_expr < literal(DBConnector.THRESHOLD)), notnull)
                        .order_by(sim_expr)
                        .limit(DBConnector.TOP_K)
                        .subquery("hnsw_candidates")
                    )
                    base = subq
                    break

        orig_cols = list(query.selected_columns)

        remapped_cols = []
        for col in orig_cols:
            if hasattr(col, "table") and col.table is orig_base:
                remapped_cols.append(base.c[col.key])
            else:
                remapped_cols.append(col)

        new_query = select(*remapped_cols).select_from(base)

        wheres = []
        for col_name, ops in filters.items():
            col = getattr(base.c, col_name, None)
            assert col is not None, f"Column {col_name!r} not found on {base.name}"
            for op, val in ops:
                assert op in DBConnector.operator_map, f"Unknown operator {op!r}"
                wheres.append(DBConnector.operator_map[op](col, val))

        if wheres:
            new_query = new_query.where(and_(*wheres))

        return new_query

    @execute
    @staticmethod
    def get_total_count(table_ref, filters=None):
        query = select(func.count()).select_from(table_ref)
        query = DBConnector.apply_filters(query, table_ref, filters)

        return query

    @execute
    @staticmethod
    def get_done_dates(table_ref, filters=None):
        query = select(
            table_ref.c[DBCOLUMNS.date].label("date"), func.count().label("freq")
        )
        query = DBConnector.apply_filters(query, table_ref, filters)

        query = query.group_by(table_ref.c[DBCOLUMNS.date])

        date_counts = query.cte("date_counts")

        median_cte = (
            select(
                func.percentile_cont(0.5)
                .within_group(date_counts.c.freq)
                .label("median_freq")
            )
            .select_from(date_counts)
            .where(date_counts.c.freq > 0)
            .cte("stats")
        )

        final_q = (
            select(date_counts.c.date)
            .select_from(date_counts.join(median_cte, true()))
            .where(date_counts.c.freq > median_cte.c.median_freq * 0.9)
            .order_by(date_counts.c.date)
        )

        return final_q

    @execute
    @staticmethod
    def get_all_rows(table_ref, filters=None, columns=None):
        columns = columns if columns else list(DBCOLUMNS)
        query = select(*(table_ref.c[col] for col in columns))
        query = DBConnector.apply_filters(query, table_ref, filters)
        return query

    @execute
    @staticmethod
    def get_archive_freq(table_ref, filters=None):
        query = select(table_ref.c[DBCOLUMNS.archive], func.count())
        query = DBConnector.apply_filters(query, table_ref, filters)
        select_from = query.get_final_froms()[0]
        query = query.group_by(select_from.c[DBCOLUMNS.archive]).order_by(
            func.count().desc()
        )
        return query

    @execute
    @staticmethod
    def get_tags(table_ref, filters=None):
        query = select(table_ref.c[DBCOLUMNS.tag]).select_from(table_ref)
        query = DBConnector.apply_filters(query, table_ref, filters)
        select_from = query.get_final_froms()[0]

        query = (
            select(
                func.trim(func.upper(select_from.c[DBCOLUMNS.tag])),
            )
            .group_by(func.trim(func.upper(select_from.c[DBCOLUMNS.tag])))
            .order_by(func.count().desc())
            .limit(100)
        )
        return query

    @execute
    @staticmethod
    def fetch_data_keyset(
        table_ref,
        last_seen_value=None,
        direction="forward",
        columns=None,
        limit=10,
        filters=None,
        desc_order=True,
    ):

        query = select(*[table_ref.c[col] for col in columns] if columns else table_ref)

        query = DBConnector.apply_filters(query, table_ref, filters)
        select_from = query.get_final_froms()[0]

        is_effective_desc = (direction == "forward" and desc_order) or (
            direction == "backward" and not desc_order
        )

        if last_seen_value:
            pagination_key_cols = [
                select_from.c[DBCOLUMNS.date],
                select_from.c[DBCOLUMNS.rowid],
            ]
            last_values_tuple = (
                last_seen_value[DBCOLUMNS.date],
                last_seen_value[DBCOLUMNS.rowid],
            )

            if is_effective_desc:
                keyset_condition = tuple_(*pagination_key_cols) < last_values_tuple
            else:
                keyset_condition = tuple_(*pagination_key_cols) > last_values_tuple

            query = query.where(keyset_condition)

        if is_effective_desc:
            order_by_clause = [
                select_from.c[DBCOLUMNS.date].desc(),
                select_from.c[DBCOLUMNS.rowid].desc(),
            ]
        else:
            order_by_clause = [
                select_from.c[DBCOLUMNS.date].asc(),
                select_from.c[DBCOLUMNS.rowid].asc(),
            ]

        query = query.order_by(*order_by_clause).limit(limit)

        return query

    @execute
    @staticmethod
    def group_by(table_ref, value, filters=None):
        assert value in ["day", "month", "year"]

        base_query = select(table_ref)
        filtered_query = DBConnector.apply_filters(base_query, table_ref, filters)
        filtered_query_cte = filtered_query.cte("filtered_query")

        query = select(
            func.date_trunc(value, filtered_query_cte.c[DBCOLUMNS.date]).label(value),
            func.count(filtered_query_cte.c[DBCOLUMNS.rowid]).label("count"),
        ).select_from(filtered_query_cte)

        query = query.group_by(value).order_by(value)
        return query

    @execute
    @staticmethod
    def get_min_max_dates(table_ref, filters=None):
        base_query = select(table_ref)
        filtered_query = DBConnector.apply_filters(base_query, table_ref, filters)

        query = select(
            func.min(filtered_query.c[DBCOLUMNS.date]),
            func.max(filtered_query.c[DBCOLUMNS.date]),
        ).select_from(filtered_query)

        return query

    @execute
    @staticmethod
    def insert_row(table_ref, values):
        insert_stmt = insert(table_ref).values(values).on_conflict_do_nothing()
        return insert_stmt

    @staticmethod
    def export_data_to_csv(engine, table, output_csv, columns):
        chunksize = 100_000
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        selected_cols = [table_ref.c[col] for col in columns]
        query = table_ref.select().with_only_columns(*selected_cols)
        with engine.execute() as connection:
            with open(output_csv, "w", encoding="utf-8", newline="") as file:
                first_chunk = True
                for chunk in pd.read_sql(query, connection, chunksize=chunksize):
                    chunk.to_csv(file, index=False, header=first_chunk)
                    first_chunk = False
