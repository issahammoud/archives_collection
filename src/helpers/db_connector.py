from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    select,
    func,
    text,
    Index,
    PrimaryKeyConstraint,
)
from sqlalchemy.sql import and_
from sqlalchemy.types import String, DateTime, LargeBinary

from src.helpers.enum import DBCOLUMNS


class DBConnector:
    DBNAME = "postgresql:///archives"
    TABLE = "sections"

    operator_map = {
        "eq": lambda column, value: column == value,
        "gt": lambda column, value: column > value,
        "lt": lambda column, value: column < value,
        "ge": lambda column, value: column >= value,
        "le": lambda column, value: column <= value,
        "in": lambda column, value: column.in_(value),
        "like": lambda column, value: column.like(value),
        "notnull": lambda column, value: column.isnot(None),
    }

    @staticmethod
    def get_engine(db_name=None, echo=False):
        db_name = db_name or DBConnector.DBNAME
        return create_engine(db_name, echo=echo)

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
                PrimaryKeyConstraint(DBCOLUMNS.rowid.value),
            )

            metadata.create_all(engine)

            index = Index("idx_date", table_ref.c.date)
            index.create(engine)

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
            for column, condition in filters.items():
                column_ref = getattr(table_ref.c, column)

                if isinstance(condition, tuple):
                    operator, value = condition
                    if operator in DBConnector.operator_map:
                        query = query.where(
                            DBConnector.operator_map[operator](column_ref, value)
                        )
                    else:
                        raise ValueError(f"Unsupported operator {operator}")
                else:
                    query = query.where(column_ref == condition)
        return query

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

    @staticmethod
    def get_archive_count(engine, table, archive, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(func.count()).where(table_ref.c.archive == archive)
            query = DBConnector.apply_filters(query, table_ref, filters)
            result = connection.execute(query)
            return result.scalar()

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

    @staticmethod
    def get_tags(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(
                func.trim(func.upper(table_ref.c.tag)).label("tag"),
                func.count().label("count"),
            )
            query = DBConnector.apply_filters(query, table_ref, filters)

            query = (
                query.group_by(func.trim(func.upper(table_ref.c.tag)))
                .order_by(func.count().desc())
                .limit(100)
            )
            result = connection.execute(query)
            return [row.tag for row in result]

    @staticmethod
    def fetch_data_keyset(
        engine,
        table,
        primary_key_column,
        last_seen_value=None,
        direction="asc",
        columns=None,
        limit=10,
        filters=None,
    ):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        order_by_clause = [
            table_ref.c.date.asc() if direction == "asc" else table_ref.c.date.desc(),
            (
                table_ref.c[primary_key_column].asc()
                if direction == "asc"
                else table_ref.c[primary_key_column].desc()
            ),
        ]

        query = select([table_ref.c[col] for col in columns] if columns else table_ref)

        query = DBConnector.apply_filters(query, table_ref, filters)

        if last_seen_value:
            query = query.where(
                (table_ref.c.date > last_seen_value["date"])
                | (
                    and_(
                        table_ref.c.date == last_seen_value["date"],
                        table_ref.c[primary_key_column]
                        > last_seen_value[primary_key_column],
                    )
                )
            )

        query = query.order_by(*order_by_clause).limit(limit)

        with engine.connect() as connection:
            result = connection.execute(query)

        return result.fetchall()

    @staticmethod
    def group_by_month(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(
                func.date_trunc("month", table_ref.c.date).label("month"),
                func.count(table_ref.c.rowid).label("count"),
            )
            query = DBConnector.apply_filters(query, table_ref, filters)
            query = query.group_by(func.date_trunc("month", table_ref.c.date)).order_by(
                func.date_trunc("month", table_ref.c.date)
            )

            result = connection.execute(query)
            return result.fetchall()

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

    @staticmethod
    def insert_row(engine, table, kwargs):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        insert_stmt = table_ref.insert().values(**kwargs)

        if "postgresql" in str(engine.url):
            from sqlalchemy.dialects.postgresql import insert

            insert_stmt = insert(table_ref).values(**kwargs).on_conflict_do_nothing()

        with engine.connect() as connection:
            connection.execute(insert_stmt)
            connection.commit()

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

    @staticmethod
    def drop_table(engine, table_name):
        metadata = MetaData()
        table_ref = Table(table_name, metadata, autoload_with=engine)

        table_ref.drop(engine)
        print(f"Table '{table_name}' has been dropped.")
