import os
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
from pgvector.sqlalchemy import Vector
from sqlalchemy.types import String, DateTime, LargeBinary


from src.helpers.enum import DBCOLUMNS
from src.utils.utils import has_table_decorator


class DBConnector:

    TABLE = "sections"
    _engine = None

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
    }

    @staticmethod
    def get_engine():
        if DBConnector._engine is None:
            user = os.getenv("POSTGRES_USER")
            password = os.getenv("POSTGRES_PASSWORD")
            db_name = os.getenv("POSTGRES_DB")
            db_url = f"postgresql://{user}:{password}@db:5432/{db_name}"
            assert None not in [
                user,
                password,
                db_name,
            ], "Failed to load the env variables"
            DBConnector._engine = create_engine(db_url)

        return DBConnector._engine

    @staticmethod
    def create_table(engine, table):
        if not DBConnector.has_table(engine, table):
            metadata = MetaData()

            table_ref = Table(
                table,
                metadata,
                Column(DBCOLUMNS.rowid.value, String, nullable=False),
                Column(DBCOLUMNS.date.value, DateTime, nullable=False),
                Column(DBCOLUMNS.archive.value, String, nullable=False),
                Column(DBCOLUMNS.image.value, LargeBinary, nullable=True),
                Column(DBCOLUMNS.title.value, String, nullable=True),
                Column(DBCOLUMNS.content.value, String, nullable=True),
                Column(DBCOLUMNS.tag.value, String, nullable=True),
                Column(DBCOLUMNS.link.value, String, nullable=False, unique=True),
                Column(
                    DBCOLUMNS.embedding.value, Vector(768), nullable=False, unique=True
                ),
                PrimaryKeyConstraint(DBCOLUMNS.rowid.value),
            )

            metadata.create_all(engine)

            index_asc = Index(
                "idx_rowid_date_asc",
                table_ref.c[DBCOLUMNS.rowid].asc(),
                table_ref.c[DBCOLUMNS.date].asc(),
            )
            index_asc.create(bind=engine)

            index_desc = Index(
                "idx_rowid_date_desc",
                table_ref.c[DBCOLUMNS.rowid].desc(),
                table_ref.c[DBCOLUMNS.date].desc(),
            )
            index_desc.create(bind=engine)

            DBConnector.add_searchable_column(engine, table, DBCOLUMNS.text_searchable)

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
                    f"CREATE INDEX {column_name}_idx ON {table} "
                    f"USING GIN ({column_name})"
                )
            )

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
    def get_done_dates(engine, table, archive, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(table_ref.c.date).where(table_ref.c.archive == archive)

            query = DBConnector.apply_filters(query, table_ref, filters)

            query = query.distinct()

            result = connection.execute(query)
            return [row[0] for row in result]

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
            return result.scalar()

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
        print(f"Table '{table}' has been dropped.")
