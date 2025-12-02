🛒 Retail Sales ETL Pipeline (Python, Pandas, SQLite, Scheduler)

This project is an end-to-end ETL pipeline built using Python, Pandas, SQLite, and the Schedule library.
It loads large retail sales data in chunks, cleans it, stores it in a database, and automatically refreshes data every day at 2:00 AM.

🚀 Features
✔ Chunk-based CSV Loading

Efficiently loads large CSV files using Pandas chunksize.

✔ Automatic Table Creation

Automatically builds raw_sales and clean_sales tables in SQLite.

✔ Data Cleaning

Removes empty rows

Standardizes column names

Handles dates

Ensures consistent schema

✔ Daily Automated ETL Job

A scheduled job runs every day at 02:00 AM using the schedule library.

✔ SQL View for Daily Revenue

A database view is created:

CREATE VIEW IF NOT EXISTS daily_revenue AS
SELECT order_date, SUM(sales) AS total_revenue
FROM clean_sales
GROUP BY order_date;

📂 Project Structure
retailsales/
│── retail.csv
│── retail.db
│── t.py
│── README.md
│── venv/

🧠 How the ETL Works
1. Load CSV in Chunks
chunks = pd.read_csv("retail.csv", chunksize=5000, parse_dates=["Order Date", "Ship Date"])

2. Insert into raw_sales
chunk.to_sql("raw_sales", conn, if_exists="append", index=False)

3. Clean Data

Remove Nulls

Fix column names

Convert dates

4. Store into clean_sales
df.to_sql("clean_sales", conn, if_exists="replace", index=False)

5. Create Revenue View
conn.execute("""CREATE VIEW IF NOT EXISTS daily_revenue AS
SELECT order_date, SUM(sales) AS total_revenue
FROM clean_sales GROUP BY order_date;""")

6. Schedule Daily ETL
schedule.every().day.at("02:00").do(update_pipeline)

🛠️ Installation & Setup
1. Clone the Repository
git clone https://github.com/yourusername/retailsales-etl.git
cd retailsales-etl

2. Create Virtual Environment
python -m venv venv

3. Activate venv

Windows

venv\Scripts\activate

4. Install Requirements
pip install pandas schedule

▶️ Run the ETL Pipeline
python t.py


You will see:

Connected to SQLite DB
Data Loaded Successfully!
Dropped X rows with missing values
ETL job scheduled at 02:00 AM...

📊 Query Daily Revenue (Example)
SELECT * FROM daily_revenue LIMIT 10;

🧩 Technologies Used

Python

Pandas

SQLite

Schedule (for automation)

Virtual Environment (venv)

🤝 Contributing

Pull requests are welcome!
If you want to improve this ETL pipeline, feel free to contribute.

⭐ Support

If you like this project, please ⭐ star the repository!
It helps others discover it.