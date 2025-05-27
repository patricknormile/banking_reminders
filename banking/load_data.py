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

conn.execute("CREATE SCHEMA IF NOT EXISTS source_data")

print(conn.sql("""
    select
        column0 as date
        , column1 as description
        , column2 as transaction_type
        , column3 as string_amount
    from p_checking
    """)
)

# p_credit = conn.read_csv("data/p_credit/2025-05-06_transaction_download.csv")
# print(p_credit)