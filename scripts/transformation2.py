import re
import pandas as pd
import mysql.connector
from datetime import datetime
from dateutil import parser as dateutil_parser

DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "admin123",
    "database": "de_database",
    "connection_timeout": 30
}

def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def get_data(conn, table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    print(f"[OK] {len(df)} rows from {table_name}")
    return df

#custom functions untuk standarisasi dan cleansing
def clean(val):
    if val is None:
        return None
    dirty = ['!','@','#','$','%','^','&','*','(',')','+','-','=',
             '[',']','{','}',';',"'",':','\\','"',',','.','/',
             '<','>','?','|','~',' ','','<null>','None','null','Null']
    cleaned = str(val).strip()
    return None if cleaned in dirty else cleaned

def standardize_date(df, col):
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d.%m.%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%m/%d/%Y",
    ]

    def parse_value(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        val = str(val).strip()
        val = re.sub(r'^[A-Za-z]+', '', val).strip()
        for fmt in formats:
            try:
                return datetime.strptime(val, fmt)
            except ValueError:
                continue
        try:
            return dateutil_parser.parse(val)
        except Exception:
            return None

    df[col] = df[col].apply(parse_value)
    return df

def standardize_price(df, col):
    def parse_price(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        cleaned = re.sub(r'[^\d.]', '', str(val))
        try:
            return float(cleaned)
        except Exception:
            return None

    df[col] = df[col].apply(parse_price)
    return df

CUSTOM_FUNCTIONS = {
    "standardize_date":  standardize_date,
    "standardize_price": standardize_price,
}

DATATYPE_MAPPING = {
    "varchar":  "VARCHAR({length})",
    "int":      "INT",
    "bigint":   "BIGINT",
    "decimal":  "DECIMAL(15,2)",
    "float":    "FLOAT",
    "date":     "DATE",
    "datetime": "DATETIME",
    "boolean":  "TINYINT(1)",
    "bit":      "TINYINT(1)",
}


def create_destination_table(conn, dest_table, metadata):
    col_definitions = []
    for _, row in metadata.iterrows():
        col   = row["DestinationColumn"]
        dtype = str(row["DestinationDataType"]).lower()
        length = row["Length"]

        sql_type = DATATYPE_MAPPING.get(dtype, "VARCHAR(255)")
        if "{length}" in sql_type:
            sql_type = sql_type.format(length=int(length) if pd.notna(length) else 255)

        col_definitions.append(f"`{col}` {sql_type}")

    ddl = f"CREATE TABLE IF NOT EXISTS `{dest_table}` ({', '.join(col_definitions)})"
    print(f"[DDL] {ddl}")

    cursor = conn.cursor()
    cursor.execute(ddl)
    conn.commit()
    cursor.close()
    print(f"[OK] Tabel {dest_table} siap")


def mapping_and_transform(df_source, metadata):
    #mapping nama kolom as is menjadi to be sesuai metadata
    rename_map = dict(zip(metadata["SourceColumn"], metadata["DestinationColumn"]))
    df = df_source.rename(columns=rename_map)

    # apply custom function per kolom
    for _, row in metadata.iterrows():
        dest_col   = row["DestinationColumn"]
        func_name  = row["CustomFunction"]
        dtype      = str(row["DestinationDataType"]).lower()

        if func_name and pd.notna(func_name) and func_name.strip() in CUSTOM_FUNCTIONS:
            print(f"[...] Applying {func_name} on {dest_col}")
            df = CUSTOM_FUNCTIONS[func_name.strip()](df, dest_col)

        # apply clean() ke semua kolom varchar
        elif dtype == "varchar":
            df[dest_col] = df[dest_col].apply(clean)

    dest_cols = metadata["DestinationColumn"].tolist()
    df = df[[col for col in dest_cols if col in df.columns]]

    return df

def load_to_mysql(conn, dest_table, df, metadata, load_type="FULL"):
    dest_cols = metadata["DestinationColumn"].tolist()
    dest_cols = [col for col in dest_cols if col in df.columns]

    cursor = conn.cursor()

    if load_type.upper() == "FULL":
        cursor.execute(f"TRUNCATE TABLE `{dest_table}`")
        print(f"[OK] Truncate {dest_table}")

    cols_str        = ", ".join([f"`{c}`" for c in dest_cols])
    placeholders    = ", ".join(["%s"] * len(dest_cols))

    inserted = 0
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(v) else v for v in [row[c] for c in dest_cols])
        cursor.execute(
            f"INSERT INTO `{dest_table}` ({cols_str}) VALUES ({placeholders})",
            values
        )
        inserted += 1

    conn.commit()
    cursor.close()
    print(f"[OK] {inserted} rows succedeed load in {dest_table}")

def run(source_object, destination_object, load_type="FULL"):
    conn = get_conn()
    print(f"\n=== {source_object} -> {destination_object} (LoadType: {load_type}) ===")

    # ambil metadata
    metadata = pd.read_sql(f"""
        SELECT SourceColumn, DestinationColumn, DestinationDataType, Length, CustomFunction
        FROM TBL_Metadata_System
        WHERE SourceObjectName  = '{source_object}'
        AND   DestinationObjectName = '{destination_object}'
        ORDER BY DestinationColumn
    """, conn)
    print(f"[OK] {len(metadata)} columns from metadata")

    # ambil data raw dari manual file csv/excel
    df_source = get_data(conn, source_object)

    # buat table hasil transformasi jika belum ada
    create_destination_table(conn, destination_object, metadata)

    # mapping & transofrm
    df_transformed = mapping_and_transform(df_source, metadata)
    print(f"[OK] transformation is done - shape: {df_transformed.shape}")

    # dump ke target table
    load_to_mysql(conn, destination_object, df_transformed, metadata, load_type)

    conn.close()
    print(f"[DONE] {destination_object} done\n")

def run_all(load_type="FULL"):
    conn = get_conn()
    mappings = pd.read_sql("""
        SELECT DISTINCT SourceObjectName, DestinationObjectName
        FROM TBL_Metadata_System
    """, conn)
    conn.close()

    print(f"[INFO] {len(mappings)} table will be transformed")
    for _, row in mappings.iterrows():
        run(row["SourceObjectName"], row["DestinationObjectName"], load_type)

if __name__ == "__main__":
    run_all()