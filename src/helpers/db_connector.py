import numpy as np
from sqlalchemy import create_engine, text, inspect
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    LargeBinary,
    DateTime,
    PrimaryKeyConstraint,
)

from src.helpers.enum import DBCOLUMNS


class DBConnector:
    DBNAME = "postgresql://issahammoud:allahma123@localhost:5432/lemonde"
    TABLE = "sections"
    VIEW = "filtered_sections"
    TABLE_VIEW = None

    @staticmethod
    def get_engine(db_name, echo=False):
        engine = create_engine(db_name, echo=echo)
        return engine

    @staticmethod
    def has_table_or_view(engine, table):
        return inspect(engine).has_table(table)

    @staticmethod
    def create_table(engine, table):
        if not DBConnector.has_table_or_view(engine, table):
            metadata = MetaData()

            Table(
                table,
                metadata,
                Column(DBCOLUMNS.rowid, String, nullable=False),
                Column(DBCOLUMNS.date, DateTime, nullable=False),
                Column(DBCOLUMNS.archive, String, nullable=False),
                Column(DBCOLUMNS.image, LargeBinary, nullable=True),
                Column(DBCOLUMNS.title, String, nullable=True),
                Column(DBCOLUMNS.content, String, nullable=True),
                Column(DBCOLUMNS.tag, String, nullable=True),
                Column(DBCOLUMNS.link, String, nullable=False, unique=True),
                PrimaryKeyConstraint(DBCOLUMNS.rowid),
            )

            metadata.create_all(engine)

            DBConnector.add_searchable_column(
                engine, DBConnector.TABLE, DBCOLUMNS.text_searchable
            )

    @staticmethod
    def add_searchable_column(engine, table, column_name):
        with engine.connect() as connection:
            connection.execute(
                text(
                    f"ALTER TABLE {table} ADD COLUMN {column_name} tsvector "
                    "GENERATED ALWAYS AS "
                    f"(to_tsvector('french', coalesce({DBCOLUMNS.title}, "
                    f"'') || ' ' || coalesce({DBCOLUMNS.content}, ''))) STORED"
                ),
            )
            connection.execute(
                text(
                    f"CREATE INDEX {column_name}_idx ON {table} "
                    f"USING GIN ({column_name})"
                )
            )
            connection.commit()

    @staticmethod
    def create_view(
        engine, table_name, view_name, tag, date_range, query, switch, source
    ):
        min_date, max_date = date_range
        data = {"date_1": min_date, "date_2": max_date}

        condition = ""
        if tag and tag != "All":
            condition = "AND TRIM(UPPER(tag)) = :tag_1 "
            data.update({"tag_1": tag.strip().upper()})

        if source:
            condition += f"AND {DBCOLUMNS.archive} IN :archives "
            data.update({"archives": tuple(source)})
        if switch:
            condition += "AND image IS NOT NULL "

        if query:
            query = query = " & ".join(query.split())
            condition += f"AND {DBCOLUMNS.text_searchable} @@ to_tsquery(:query)"
            data.update({"query": query})

        with engine.connect() as connection:
            connection.execute(
                text(
                    f"CREATE OR REPLACE view {view_name} AS "
                    f"(SELECT * FROM {table_name}"
                    " WHERE date >= :date_1 "
                    f"AND date <= :date_2 {condition} "
                    f"ORDER BY date, {DBCOLUMNS.rowid})"
                ),
                data,
            )

            connection.commit()

    @staticmethod
    def drop_table(engine, table):
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE {table}"))
            connection.commit()

    @staticmethod
    def drop_view(engine, view):
        with engine.connect() as connection:
            connection.execute(text(f"DROP VIEW {view}"))
            connection.commit()

    @staticmethod
    def get_first_n_rows(engine, table, n, columns):
        columns = ", ".join(columns) if columns else "*"

        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT {columns} FROM "
                    f"(SELECT {columns} FROM {table} ORDER BY date, {DBCOLUMNS.rowid}) "
                    "AS sorted_table LIMIT :n"
                ),
                {"id": id, "n": n},
            )
            rows = result.fetchall()

        return rows

    @staticmethod
    def get_next_n_rows(engine, table, id, n, columns=None):
        columns = ", ".join(columns) if columns else "*"

        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT {columns} FROM "
                    f"(SELECT {columns} FROM {table} ORDER BY date, {DBCOLUMNS.rowid}) "
                    "AS sorted_table "
                    f"WHERE {DBCOLUMNS.rowid} > :id  LIMIT :n"
                ),
                {"id": id, "n": n},
            )
            rows = result.fetchall()

        return rows

    @staticmethod
    def get_all_rows(engine, table, columns=None):
        columns = ", ".join(columns) if columns else "*"
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT {columns} FROM {table} ORDER BY date, {DBCOLUMNS.rowid}"),
            )
            rows = result.fetchall()

        return rows

    @staticmethod
    def get_tags(engine, table):
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT TRIM(UPPER({DBCOLUMNS.tag})) FROM {table} "
                    f"GROUP BY TRIM(UPPER({DBCOLUMNS.tag})) "
                    "ORDER BY COUNT(*) DESC limit 100"
                )
            )
            rows = result.fetchall()

        return rows

    @staticmethod
    def get_archive_rows(engine, table, archive, columns=None):
        columns = ", ".join(columns) if columns else "*"
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT {columns} FROM {table} WHERE archive = :arx"),
                {"arx": archive},
            )
            rows = result.fetchall()

        return rows

    @staticmethod
    def get_archive_count(engine, table, archive):
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT COUNT(*) FROM {table} WHERE archive = :arx"),
                {"arx": archive},
            )
            count = result.fetchone()[0]

        return count

    @staticmethod
    def get_archive_freq(engine, table):
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT archive, COUNT(*) FROM {table} "
                    "GROUP BY archive ORDER BY COUNT(*) DESC"
                )
            )
            rows = result.fetchall()

        return np.array(rows)

    @staticmethod
    def insert_row(engine, table, kwargs):
        with engine.connect() as connection:
            keys = list(kwargs.keys())
            placeholders = [":" + key for key in keys]

            insert_query = (
                f"INSERT INTO {table} "
                f"({', '.join(keys)}) "
                f"VALUES({', '.join(placeholders)}) "
                "ON CONFLICT DO NOTHING"
            )
            connection.execute(text(insert_query), kwargs)
            connection.commit()

    @staticmethod
    def get_min_max_dates(engine, table):
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT MIN(date) FROM {table}"),
            )
            min_date = result.fetchone()[0]

            result = connection.execute(
                text(f"SELECT MAX(date) FROM {table}"),
            )
            max_date = result.fetchone()[0]

        return [min_date.date(), max_date.date()]

    @staticmethod
    def get_done_dates(engine, table, archive):
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT DISTINCT date FROM {table} WHERE {DBCOLUMNS.archive}=:arx"
                ),
                {"arx": archive},
            )
            dates = result.fetchall()
        return dates

    @staticmethod
    def get_total_rows_count(engine, table):
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
            dates = result.fetchone()[0]
        return dates

    @staticmethod
    def group_by_month(engine, table):
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT DATE_TRUNC('month', {DBCOLUMNS.date}) AS month, "
                    f"COUNT({DBCOLUMNS.rowid}) AS COUNT FROM {table} GROUP BY month "
                    "ORDER BY month"
                ),
            )
            rows = result.fetchall()
        return rows
