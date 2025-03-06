import os
from multiprocessing import Pool, cpu_count
from wand.image import Image
from io import BytesIO
from sqlalchemy import MetaData, Table, select, delete, insert, func, and_
from sqlalchemy.sql import text
from src.helpers.db_connector import DBConnector
from src.helpers.enum import DBCOLUMNS


def save_image(row, quality=80):
    blob = row[DBCOLUMNS.image]
    file_path = None

    if isinstance(blob, bytes):
        date = row[DBCOLUMNS.date]
        try:
            year = date.year
            month = date.month
        except Exception as e:
            # Fallback if parsing fails
            year = "unknown"
            month = "unknown"

        subdir = os.path.join(BASE_IMAGE_DIR, str(year), str(month))
        os.makedirs(subdir, exist_ok=True)
        file_name = f"{row[DBCOLUMNS.rowid]}.webp"
        file_path = os.path.join(subdir, file_name)

        try:
            with Image(file=BytesIO(blob)) as img:
                img.quality = quality
                img.format = 'webp'
                img.save(filename=file_path)
        except Exception:
            print("exception")
            with open(file_path, "wb") as file:
                file.write(blob) 


        row_dict = dict(row)
        row_dict[DBCOLUMNS.image] = file_path
        row_dict.pop(DBCOLUMNS.text_searchable)
        return row_dict


def get_updated_rows(batch):
    with Pool(cpu_count()) as pool:
        updated_rows = pool.map(save_image, batch)
    return updated_rows



if __name__ == "__main__":
    engine = DBConnector.get_engine()
    metadata = MetaData()

    old_table = Table(DBConnector.TABLE, metadata, autoload_with=engine)

    new_table = Table("new_sections", metadata, autoload_with=engine) #DBConnector.create_table(engine, "new_sections")

    BASE_IMAGE_DIR = "/images/"
    os.makedirs(BASE_IMAGE_DIR, exist_ok=True)

    BATCH_SIZE = 10000

    while True:
        with engine.connect() as conn:
            stmt = select(*[old_table.c[col] for col in list(DBCOLUMNS)]).where(old_table.c[DBCOLUMNS.image] != None).limit(BATCH_SIZE)
            batch = conn.execute(stmt).mappings().all()
            if not batch:
                print("All rows have been processed.")
                break

            updated_rows = get_updated_rows(batch)
            ins_stmt = insert(new_table).values(updated_rows)
            conn.execute(ins_stmt)

            # Collect primary keys from the processed batch.
            ids_to_delete = [row[DBCOLUMNS.rowid] for row in batch]
            # Delete processed rows from the old table to free up space.
            del_stmt = delete(old_table).where(old_table.c[DBCOLUMNS.rowid].in_(ids_to_delete))
            conn.execute(del_stmt)

            count_stmt = select(func.count()).select_from(old_table)
            count_1 = conn.execute(count_stmt).scalar()
            count_stmt = select(func.count()).select_from(new_table)
            count_2 = conn.execute(count_stmt).scalar()
            print(f"Processed and removed {len(batch)} rows.")
            print(f"Remaining in old table {count_1}. Inserted into new table: {count_2}")
            conn.commit()
    # while True:
    #     with engine.connect() as conn:
    #         select_stmt = select(
    #                 *[old_table.c[col] for col in list(DBCOLUMNS) if "text_searchable" not in col.value]
    #             ).where(old_table.c[DBCOLUMNS.image].is_(None)).limit(BATCH_SIZE)
    #         rows = conn.execute(select_stmt).mappings().all()
            
    #         if rows:
    #             insert_stmt = insert(new_table).values([dict(row) for row in rows])
    #             conn.execute(insert_stmt)

    #             ids_to_delete = [row[DBCOLUMNS.rowid] for row in rows]
    #             del_stmt = delete(old_table).where(old_table.c[DBCOLUMNS.rowid].in_(ids_to_delete))
    #             conn.execute(del_stmt)

    #         count_stmt = select(func.count()).select_from(old_table)
    #         count_1 = conn.execute(count_stmt).scalar()
    #         count_stmt = select(func.count()).select_from(new_table)
    #         count_2 = conn.execute(count_stmt).scalar()
    #         print(f"Remaining in old table {count_1}. Inserted into new table: {count_2}")
    #         conn.commit()

    # After processing all rows, drop the old table and rename the new table.
    # with engine.connect() as conn:
    #     conn.execute(text("DROP TABLE sections;"))
    #     conn.execute(text("ALTER TABLE new_sections RENAME TO sections;"))

    # print("Migration completed: image BLOBs replaced by file paths with subdirectory structure based on date.")
