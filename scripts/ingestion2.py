print("FILE LOADED")
import pandas as pd
import mysql.connector
import time

DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "admin123",
    "database": "de_database",
    "connection_timeout": 30
}

TABLES = {
    "customers_raw": {
        "source": "excel",
        "file_path": "data/customers_data.xlsx",
        "sheet": "customers_raw",
        "query": """
                CREATE TABLE IF NOT EXISTS customers_raw (
                id INT,
                name VARCHAR(255),
                dob VARCHAR(50),
                created_at DATETIME(6)
                )
                """
    }
    , "sales_raw": {
        "source": "excel",
        "file_path": "data/customers_data.xlsx",
        "sheet": "sales_raw",
        "query": """
                CREATE TABLE IF NOT EXISTS sales_raw (
                vin VARCHAR(255),
                customer_id INT,
                model VARCHAR(50),
                invoice_date VARCHAR(50),
                price VARCHAR(255),
                created_at DATETIME(2)
                )
                """
    }
    , "after_sales_raw": {
        "source": "excel",
        "file_path": "data/customers_data.xlsx",
        "sheet": "after_sales_raw",
        "query": """
                CREATE TABLE IF NOT EXISTS after_sales_raw (
                service_ticket VARCHAR(50),
                vin VARCHAR(255),
                customer_id INT,
                model VARCHAR(255),
                service_date VARCHAR(50),
                service_type VARCHAR(50),
                created_at DATETIME(6)
                )
                """
    }
    , "customer_addresses": {
        "source": "csv",
        "file_path": "data/customer_addresses.csv", 
        "sheet": None,
        "query": """
                CREATE TABLE IF NOT EXISTS customer_addresses (
                id INT,
                customer_id INT,
                address VARCHAR(255),
                city VARCHAR(50),
                province VARCHAR(50),
                created_at DATETIME(6)
                )
                """
    }

}

def get_conn():
    conn = mysql.connector.connect(**DB_CONFIG)
    print("[OK] MySQL Connection Succeeded")
    return conn

def create_table(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()

def ingestion(conn, table_name, sheet_name=None, file_path=None, source="excel"):
    if source == "excel":
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    elif source == "csv":
        df = pd.read_csv(file_path, delimiter=';')

    df = df.where(pd.notnull(df), None)

    cols = ", ".join(df.columns)
    insert_data = ", ".join(["%s"] * len(df.columns))
    
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    try:
        for _, row in df.iterrows():
            cursor.execute(
                f"INSERT INTO {table_name}({cols}) VALUES ({insert_data})",
                tuple(row) 
            )
        
        conn.commit()
        cursor.close()
        print(f"Data Inserted")
    except Exception as e:
        print(f"An unexpected error occured: {e}")

def run():
    try:
        conn = get_conn()
        print("[OK] MySQL Connection Succeeded")
    except Exception as e:
        print(f"[ERROR] Failed to connect MySQL: {e}")
        return

    for table_name, config in TABLES.items():
        print(f"[...] Processing {table_name}")
        create_table(conn, config["query"])
        ingestion(conn, 
                  table_name, 
                  sheet_name=config["sheet"],
                  file_path=config["file_path"],
                  source=config["source"]
                  )
    
    conn.close()
    print("[DONE] All Raw Data is ingested")

if __name__ == "__main__":
    run()
