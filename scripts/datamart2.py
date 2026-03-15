import mysql.connector
import pandas as pd

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

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DM_SalesReport (
            Period     VARCHAR(10),
            Class       VARCHAR(10),
            Model       VARCHAR(50),
            Total       DECIMAL(15,6)
        )            
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DM_AfterSalesReport (
            Period         VARCHAR(10),
            Vin             VARCHAR(255),
            CustomerName   VARCHAR(255),
            Address         VARCHAR(255),
            CountService   INT,
            Priority        VARCHAR(10)
        )
    """)
    conn.commit()
    cursor.close()
    print(f"Table created")

def load_data(conn):
    cursor = conn.cursor()

    # DM_SalesReport
    cursor.execute("TRUNCATE TABLE DM_SalesReport")
    try:
        cursor.execute("""
            INSERT INTO DM_SalesReport (Period, Class, Model, Total)
            SELECT 
                DATE_FORMAT(InvoiceDate, '%Y-%m') AS Period,
                CASE 
                    WHEN Price BETWEEN 100000000 AND 250000000 THEN 'LOW'
                    WHEN Price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
                    WHEN Price > 400000000 THEN 'HIGH'
                END AS Class,
                Model,
                SUM(Price) AS Total
            FROM Sales
            GROUP BY 
                DATE_FORMAT(InvoiceDate, '%Y-%m'),
                Class,
                Model
            ORDER BY Period, Class, Model
        """)
        print("[OK] DM_SalesReport selesai")
    except Exception as e:
        print(f"[ERROR] DM_SalesReport: {e}")

    # DM_AfterSalesReport
    cursor.execute("TRUNCATE TABLE DM_AfterSalesReport")
    try:
        cursor.execute("""
            INSERT INTO DM_AfterSalesReport (Period, Vin, CustomerName, Address, CountService, Priority)
            WITH cte AS (
                SELECT 
                    t.Vin,
                    c.Name,
                    ca.Address,
                    t.ServiceTicket,
                    t.ServiceDate,
                    t.ServiceType
                FROM AfterSales t
                LEFT JOIN Customers c ON t.CustomerID = c.ID
                LEFT JOIN CustomerAddresses ca ON c.ID = ca.CustomerID
            ),
            service_count AS (
                SELECT Vin, COUNT(ServiceType) AS CountService
                FROM AfterSales
                GROUP BY Vin
            )
            SELECT
                DATE_FORMAT(cte.ServiceDate, '%Y-%m') AS Period,
                cte.Vin,
                cte.Name AS CustomerName,
                cte.Address,
                sc.CountService,
                CASE 
                    WHEN sc.CountService > 10 THEN 'HIGH'
                    WHEN sc.CountService BETWEEN 5 AND 10 THEN 'MED'
                    ELSE 'LOW'
                END AS Priority
            FROM cte
            LEFT JOIN service_count sc ON cte.Vin = sc.Vin
            GROUP BY
                DATE_FORMAT(cte.ServiceDate, '%Y-%m'),
                cte.Vin,
                CustomerName,
                Address,
                sc.CountService,
                Priority
            ORDER BY Period, Priority
        """)
        print("[OK] DM_AfterSalesReport selesai")
    except Exception as e:
        print(f"[ERROR] DM_AfterSalesReport: {e}")

    conn.commit()
    cursor.close()

def run():
    conn = get_conn()
    create_tables(conn)
    load_data(conn)
if __name__ == "__main__":
    run()

