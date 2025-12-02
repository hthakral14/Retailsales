import pandas as pd 
import sqlite3 
import schedule
import time

conn=sqlite3.connect('retail.db')
cursor = conn.cursor()

print("Connected to SQLite DB")
conn.execute("DROP TABLE IF EXISTS raw_sales;")
conn.commit()

df_all=pd.read_csv("retail.csv",nrows=1)
all_cols=df_all.columns

chunks = pd.read_csv("retail.csv", chunksize=20000, parse_dates=["Order Date", "Ship Date"])


for chunk in chunks:
    chunk=chunk.reindex(columns=all_cols)
    chunk.to_sql("raw_sales", conn, if_exists="append", index=False)


print("Data Loaded Successfully!")

df=pd.read_sql("Select * from raw_sales",conn)
df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()


before = len(df)
df.dropna(inplace=True)
after = len(df)
print(f"Dropped {before - after} rows with missing values")


df.to_sql("clean_sales",conn,if_exists="replace",index=False)

create_view_query = """
CREATE VIEW IF NOT EXISTS daily_revenue AS
SELECT 
    order_date,
    SUM(revenue) AS total_revenue
FROM clean_sales
GROUP BY order_date;
"""

cursor.execute(create_view_query)
conn.commit()

print("daily_revenue view created successfully!")

def update_pipeline():
    print("running etl.....")

schedule.every().day.at("02:00").do(update_pipeline)

while True:
    schedule.run_pending()
    time.sleep(1)

