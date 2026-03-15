import pandas as pd
import mysql.connector

DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "admin123",
    "database": "de_database",
    "connection_timeout": 30
}

FILE_PATH = "data/transformation_metadata.xlsx"
TABLE_NAME = "TBL_Metadata_System"

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            ID                      VARCHAR(100),
            SourceObjectName        VARCHAR(100),
            DestinationObjectName   VARCHAR(100),
            SourceColumn            VARCHAR(100),
            DestinationColumn       VARCHAR(100),
            Length                  INT,
            DestinationDataType     VARCHAR(50),
            CustomFunction          VARCHAR(100)
        )
    """)
    conn.commit()
    cursor.close()
    print(f"[OK] Tabel {TABLE_NAME} siap")

def upload(conn):
    df = pd.read_excel(FILE_PATH)
    df = df.where(pd.notnull(df), None)
    df["Length"] = df["Length"].apply(lambda x: int(x) if pd.notna(x) else None)

    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")

    for _, row in df.iterrows():
        values = tuple(None if pd.isna(v) else v for v in row)
        # konversi Length ke int kalau tidak None
        values = list(values)
        values[4] = int(values[4]) if values[4] is not None else None
        
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (
                SourceObjectName, DestinationObjectName, SourceColumn,
                DestinationColumn, Length, DestinationDataType, CustomFunction
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, tuple(values))

    conn.commit()
    cursor.close()
    print(f"[OK] {len(df)} rows metadata succedeed to upload in {TABLE_NAME}")

def run():
    print("[...] Connecting to MySQL")
    conn = get_conn()
    create_table(conn)
    upload(conn)
    conn.close()
    print("[DONE] Upload metadata")

if __name__ == "__main__":
    run()