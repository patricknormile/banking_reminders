import os
import sys
from pathlib import Path
import duckdb
conn = duckdb.connect("financedb.duckdb")

root = str(Path(__file__).parent.parent)
if root not in sys.path:
    sys.path.append(root)

def create_table(
    conn: duckdb.duckdb.DuckDBPyConnection,
    table_name: str, 
    sql: str, 
    schema: str = None
):
    if schema:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    table_name = f"{schema}.{table_name}" if schema else table_name
    query = f"""CREATE OR REPLACE TABLE {table_name} AS 
    {sql};"""

    conn.execute(query)

with open(os.path.join(root, "sql","models","intermediate","checking_accounts.sql")) as f:
    sql = f.read()
    create_table(conn, "checking_accounts", sql, "intermediate")
    res = conn.execute("SELECT * FROM intermediate.checking_accounts")
    print(res.fetchall())
