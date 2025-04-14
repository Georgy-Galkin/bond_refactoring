import pyodbc

def connect_to_sql_server(server: str, database: str, driver: str = 'ODBC Driver 17 for SQL Server'):
    """
    Establishes a connection to a SQL Server database using Windows Authentication.

    Args:
        server (str): SQL Server name or IP.
        database (str): Target database name.
        driver (str): ODBC driver name (default is 'ODBC Driver 17 for SQL Server').

    Returns:
        tuple: (connection object, cursor object)

    Raises:
        Exception: If connection fails.
    """
    connection_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(connection_str)
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        raise Exception(f"Failed to connect to {server}\\{database}: {e}")


import pyodbc
import os

def load_csv_to_sql(
    conn_str: str,
    table_name: str,
    table_schema: str,
    csv_path: str,
    field_separator: str,
    columns: list,
    truncate_before_load: bool = True,
    rows_per_batch: int = 1000000,
    row_separator: str = '0x0a',
    codepage: str = '65001'
):
    """
    Loads a CSV file into SQL Server using BULK INSERT, with support for table schema.

    Args:
        table_name (str): Target table name.
        csv_path (str): Full path to CSV file.
        columns (list): List of column names to define schema (all will be NVARCHAR(MAX)).
        conn_str (str): SQL Server connection string.
        table_schema (str): SQL schema name (default: 'dbo').
        truncate_before_load (bool): Truncate table before loading.
        rows_per_batch (int): Number of rows per batch in BULK INSERT.
        row_separator (str): Row separator (default: '0x0a').
        field_separator (str): Field separator (default: '|').
        codepage (str): CSV encoding code page (default: '65001').
    """
    table_full_name = f"[{table_schema}].[{table_name}]"

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Create schema if it doesn't exist
    cursor.execute(f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.schemas WHERE name = '{table_schema}'
    )
    EXEC('CREATE SCHEMA [{table_schema}]')
    """)

    # Create table if not exists
    columns_sql = ",\n".join([f"[{col}] NVARCHAR(MAX) NULL" for col in columns])
    create_table_sql = f"""
    IF OBJECT_ID('{table_full_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_full_name} (
            {columns_sql}
        )
    END
    """
    cursor.execute(create_table_sql)

    # Optional truncate
    if truncate_before_load:
        cursor.execute(f"TRUNCATE TABLE {table_full_name}")

    # Run BULK INSERT
    bulk_sql = f"""
    BULK INSERT {table_full_name}
    FROM '{csv_path}'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '{field_separator}',
        ROWTERMINATOR = '{row_separator}',
        CODEPAGE = '{codepage}',
        KEEPNULLS,
        BATCHSIZE = {rows_per_batch},
        MAXERRORS = 0
    )
    """
    cursor.execute(bulk_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ CSV loaded into table {table_full_name}")
