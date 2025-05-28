### initial data load
import duckdb

conn = duckdb.connect("financedb.duckdb")

try:
    p_checking = conn.read_csv("data/p_checking/FirstbankDownload1746566316260.csv").\
        to_table("p_checking")
except:
    pass

try:
    joint_checking = conn.read_csv("data/joint_checking/FirstbankDownload1746566464791.csv").\
        to_table("joint_checking")
except:
    pass

try:
    p_credit = conn.read_csv("data/p_credit/2025-05-06_transaction_download.csv").\
        to_table("p_credit")
except:
    pass

try:
    m_credit = conn.read_csv("data/m_credit/2025-05-06_transaction_download.csv").\
        to_table("m_credit")
except:
    pass


print(conn.sql("""
    select
        column0 as date
        , column1 as description
        , column2 as transaction_type
        , column3 as string_amount
    from p_checking
    """)
)

print(conn.sql("""
    select
        column0 as date
        , column1 as description
        , column2 as transaction_type
        , column3 as string_amount
    from joint_checking
    """)
)

print(conn.sql("""
    select
        *
    from p_credit
    """)
)

conn.execute("CREATE SCHEMA IF NOT EXISTS source_data")

conn.execute("CREATE TABLE IF NOT EXISTS source_data.patrick_checking AS SELECT * FROM p_checking")
conn.execute("CREATE TABLE IF NOT EXISTS source_data.joint_checking AS SELECT * FROM joint_checking")
conn.execute("CREATE TABLE IF NOT EXISTS source_data.patrick_credit AS SELECT * FROM p_credit")
conn.execute("CREATE TABLE IF NOT EXISTS source_data.michaela_credit AS SELECT * FROM m_credit")
# p_credit = conn.read_csv("data/p_credit/2025-05-06_transaction_download.csv")
# print(p_credit)