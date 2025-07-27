import os
import numpy as np
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
    literal_column,
)
from sqlalchemy.sql import and_
from sqlalchemy.pool import QueuePool
from pgvector.sqlalchemy import Vector, HALFVEC
from sqlalchemy.types import String, Date, Text
from sqlalchemy.dialects.postgresql import insert, REGCONFIG

from src.utils.logging import logging
from src.helpers.enum import DBCOLUMNS, OPERATORS
from src.utils.utils import execute

logger = logging.getLogger(__name__)


class DBManager:
    _engine = None
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBManager, cls).__new__(cls)
        return cls._instance

    @property
    def engine(self):
        """Get the singleton database engine"""
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @staticmethod
    def _create_engine():
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        db_url = f"postgresql://{user}:{password}@pgbouncer:5432/{db_name}"

        assert None not in [user, password, db_name], "Failed to load the env variables"

        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=False,
            echo=False,
        )

        return engine


class DBConnector:
    TABLE = "articles"
    VECTOR_DIM = 1024

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
                    """
                CREATE OR REPLACE FUNCTION unaccent_immutable(txt text)
                RETURNS text
                LANGUAGE SQL
                IMMUTABLE
                AS $$
                SELECT unaccent(txt);
                $$;
                """
                )
            )
            connection.execute(
                text(
                    f"ALTER TABLE {table} "
                    f"ADD COLUMN {column_name} tsvector "
                    "GENERATED ALWAYS AS ("
                    "to_tsvector('french', unaccent_immutable(coalesce(title, ''))) || "
                    "to_tsvector('french', unaccent_immutable(coalesce(content, '')))"
                    ") STORED"
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
        return DynamicFilters.apply(query, table_ref, filters)

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


class DynamicFilters:
    TOP_K = os.getenv("HNSW_EF_SEARCH", 100)
    THRESHOLD = 0
    VECTOR_WEIGHT = 2.0
    BM25_WEIGHT = 1.0

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
        OPERATORS.ts: lambda column, value: column.op("@@")(
            func.plainto_tsquery(
                literal_column("'french'").cast(REGCONFIG),
                func.unaccent_immutable(bindparam("text_query", value)),
            )
        ),
        OPERATORS.vs: lambda column, value: column.op("<#>")(
            bindparam("query", value, type_=Vector(DBConnector.VECTOR_DIM))
        ),
    }

    @staticmethod
    def apply(query, table_ref, filters):
        """Main entry point for applying filters to a query."""
        if not filters:
            return query

        filters = dict(filters)
        sim_key = DBCOLUMNS.embedding
        ts_key = DBCOLUMNS.text_searchable

        has_text_search = ts_key in filters
        has_vector_search = sim_key in filters

        if has_text_search and has_vector_search:
            ts_ops = filters.pop(ts_key)
            vec_ops = filters.pop(sim_key)
            base = DynamicFilters._create_rrf_subquery_optimized(
                table_ref, ts_ops, vec_ops
            )

        elif has_text_search:
            ts_ops = filters.pop(ts_key)
            base = DynamicFilters._create_text_search_base(table_ref, ts_ops)

        elif has_vector_search:
            vec_ops = filters.pop(sim_key)
            base = DynamicFilters._create_vector_search_base(table_ref, vec_ops)

        else:
            base = table_ref

        return DynamicFilters._build_final_query(query, table_ref, base, filters)

    @staticmethod
    def _create_text_search_base(table_ref, ts_ops):
        """Create base query for text search without normalization to avoid OOM."""
        ts_op, ts_query = ts_ops[0]

        tsq = func.plainto_tsquery(
            literal_column("'french'").cast(REGCONFIG),
            func.unaccent_immutable(bindparam("text_query", ts_query)),
        )

        raw_text = func.ts_rank_cd(
            table_ref.c[DBCOLUMNS.text_searchable], tsq, 2
        ).label("text_score")
        ts_query_match = DynamicFilters.operator_map[ts_op](
            table_ref.c[DBCOLUMNS.text_searchable], ts_query
        )

        text_base = (
            select(*table_ref.c, raw_text)
            .where(ts_query_match)
            .order_by(raw_text.desc())
            .limit(DynamicFilters.TOP_K)
            .subquery("text_search_base")
        )

        return (text_base,)

    @staticmethod
    def _create_vector_search_base(table_ref, vec_ops):
        """Create base query for vector search only."""
        op, vec = vec_ops[0]

        vec_score = DynamicFilters.operator_map[op](
            table_ref.c[DBCOLUMNS.embedding], vec
        ).label("vec_score")
        notnull = DynamicFilters.operator_map[OPERATORS.notnull](
            table_ref.c[DBCOLUMNS.embedding], None
        )

        subq = (
            select(*table_ref.c, vec_score)
            .where(
                and_(
                    notnull,
                    vec_score < literal(DynamicFilters.THRESHOLD),
                )
            )
            .order_by(vec_score.asc())
            .limit(DynamicFilters.TOP_K)
            .subquery("vector_candidates")
        )
        return subq

    @staticmethod
    def _create_rrf_subquery_optimized(table_ref, ts_ops, vec_ops):
        ts_op, ts_query = ts_ops[0]
        vec_op, vec = vec_ops[0]

        RRF_K = 60

        tsq = func.plainto_tsquery(
            literal_column("'french'").cast(REGCONFIG),
            func.unaccent_immutable(bindparam("text_query", ts_query)),
        )

        text_score = func.ts_rank_cd(table_ref.c[DBCOLUMNS.text_searchable], tsq, 2)
        ts_match = DynamicFilters.operator_map[ts_op](
            table_ref.c[DBCOLUMNS.text_searchable], ts_query
        )

        text_ranked = (
            select(
                table_ref.c[DBCOLUMNS.rowid],
                text_score.label("text_score"),
                func.row_number().over(order_by=text_score.desc()).label("text_rank"),
            )
            .where(ts_match)
            .order_by(text_score.desc())
            .limit(DynamicFilters.TOP_K)
            .cte("text_ranked")
        )

        vec_score = DynamicFilters.operator_map[vec_op](
            table_ref.c[DBCOLUMNS.embedding], vec
        )
        vec_notnull = DynamicFilters.operator_map[OPERATORS.notnull](
            table_ref.c[DBCOLUMNS.embedding], None
        )

        vector_ranked = (
            select(
                table_ref.c[DBCOLUMNS.rowid],
                vec_score.label("vec_score"),
                func.row_number().over(order_by=vec_score.asc()).label("vec_rank"),
            )
            .where(
                and_(
                    vec_notnull,
                    vec_score < literal(DynamicFilters.THRESHOLD),
                )
            )
            .order_by(vec_score.asc())
            .limit(DynamicFilters.TOP_K)
            .cte("vector_ranked")
        )

        text_contribution = select(
            text_ranked.c[DBCOLUMNS.rowid],
            (
                literal(DynamicFilters.BM25_WEIGHT) / (RRF_K + text_ranked.c.text_rank)
            ).label("contribution"),
        )

        vector_contribution = select(
            vector_ranked.c[DBCOLUMNS.rowid],
            (
                literal(DynamicFilters.BM25_WEIGHT) / (RRF_K + vector_ranked.c.vec_rank)
            ).label("contribution"),
        )

        all_contributions = text_contribution.union_all(vector_contribution).cte(
            "all_contributions"
        )

        rrf_scores = (
            select(
                all_contributions.c[DBCOLUMNS.rowid],
                func.sum(all_contributions.c.contribution).label("rrf_score"),
            )
            .group_by(all_contributions.c[DBCOLUMNS.rowid])
            .order_by(func.sum(all_contributions.c.contribution).desc())
            .limit(DynamicFilters.TOP_K)
            .cte("rrf_scores")
        )

        return (
            select(*table_ref.c, rrf_scores.c.rrf_score)
            .select_from(
                table_ref.join(
                    rrf_scores,
                    table_ref.c[DBCOLUMNS.rowid] == rrf_scores.c[DBCOLUMNS.rowid],
                )
            )
            .order_by(rrf_scores.c.rrf_score.desc())
            .limit(DynamicFilters.TOP_K)
            .subquery("rrf_candidates")
        )

    @staticmethod
    def _build_final_query(
        original_query,
        orig_table_ref,
        base_table_ref,
        remaining_filters,
    ):
        remapped_cols = DynamicFilters._remap_columns(
            original_query, orig_table_ref, base_table_ref
        )

        new_query = select(*remapped_cols).select_from(base_table_ref)

        where_conditions = []

        for col_name, ops in remaining_filters.items():
            col = getattr(base_table_ref.c, col_name, None)
            assert (
                col is not None
            ), f"Column {col_name!r} not found on {base_table_ref.name}"

            for op, val in ops:
                assert op in DynamicFilters.operator_map, f"Unknown operator {op!r}"
                where_conditions.append(DynamicFilters.operator_map[op](col, val))

        if where_conditions:
            new_query = new_query.where(and_(*where_conditions))

        return new_query

    @staticmethod
    def _remap_columns(original_query, orig_table_ref, new_table_ref):
        orig_cols = list(original_query.selected_columns)
        remapped_cols = []

        for col in orig_cols:
            if hasattr(col, "table") and col.table is orig_table_ref:
                remapped_cols.append(new_table_ref.c[col.key])
            else:
                remapped_cols.append(col)

        return remapped_cols
