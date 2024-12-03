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
        "notnull": lambda column, _: column.isnot(None),
        "isnull": lambda col, _: col.is_(None),
        "text_search": lambda col, value: text(
            f"{col.name} @@ to_tsquery(:query)"
        ).bindparams(query=" & ".join(value.split())),
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

    @staticmethod
    def get_total_count(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(func.count()).select_from(table_ref)

            query = DBConnector.apply_filters(query, table_ref, filters)

            total_count = connection.execute(query).scalar()
        return total_count

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
        last_seen_value=None,
        columns=None,
        limit=10,
        filters=None,
        flip_order=False,
    ):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        if flip_order:
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
            if last_seen_value["direction"] == "forward" and flip_order:
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

        return result.fetchall()

    @staticmethod
    def group_by_day(engine, table, filters=None):
        metadata = MetaData()
        table_ref = Table(table, metadata, autoload_with=engine)

        with engine.connect() as connection:
            query = select(
                func.date_trunc("day", table_ref.c.date).label("day"),
                func.count(table_ref.c.rowid).label("count"),
            )
            query = DBConnector.apply_filters(query, table_ref, filters)
            query = query.group_by(func.date_trunc("day", table_ref.c.date)).order_by(
                func.date_trunc("day", table_ref.c.date)
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
