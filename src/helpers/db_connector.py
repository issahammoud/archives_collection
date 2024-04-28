from sqlalchemy import create_engine, text, inspect
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    LargeBinary,
    DateTime,
    PrimaryKeyConstraint,
)

from src.helpers.enum import DBCOLUMNS


class DBConnector:
    DBNAME = "postgresql://issahammoud:allahma123@localhost:5432/lemonde"
    TABLE = "sections"

    @staticmethod
    def get_engine(db_name, echo=False):
        engine = create_engine(db_name, echo=echo)
        return engine

    @staticmethod
    def has_table(engine, table):
        return inspect(engine).has_table(table)

    @staticmethod
    def create_table(engine, table):
        if not DBConnector.has_table(engine, table):
            metadata = MetaData()

            Table(
                table,
                metadata,
                Column(DBCOLUMNS.rowid, Integer, nullable=False, autoincrement=True),
                Column(DBCOLUMNS.date, DateTime, nullable=False),
                Column(DBCOLUMNS.archive, String, nullable=False),
                Column(DBCOLUMNS.image, LargeBinary, nullable=True),
                Column(DBCOLUMNS.title, String, nullable=True),
                Column(DBCOLUMNS.content, String, nullable=True),
                Column(DBCOLUMNS.tag, String, nullable=True),
                Column(DBCOLUMNS.link, String, nullable=True, unique=True),
                PrimaryKeyConstraint(DBCOLUMNS.rowid),
            )

            metadata.create_all(engine)

    @staticmethod
    def drop_table(engine, table):
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE {table}"))
            connection.commit()

    @staticmethod
    def get_row(engine, table, id, columns=None):
        columns = ", ".join(columns) if columns else "*"
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT {columns} FROM {table} WHERE {DBCOLUMNS.rowid} = :id"),
                {"id": id},
            )
            row = result.fetchone()

        return row

    @staticmethod
    def get_all_rows(engine, table, columns=None):
        columns = ", ".join(columns) if columns else "*"
        with engine.connect() as connection:
            result = connection.execute(
                text(f"SELECT {columns} FROM {table}"),
            )
            rows = result.fetchall()

        return rows

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
    def get_count(engine, table, archive):
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {table} WHERE {DBCOLUMNS.archive}=:archive"
                ),
                {"archive": archive},
            )
            dates = result.fetchone()[0]
        return dates

    @staticmethod
    def get_total_rows_count(engine, table):
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table}"))
            dates = result.fetchone()[0]
        return dates
