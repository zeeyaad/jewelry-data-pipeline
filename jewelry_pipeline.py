from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import matplotlib
matplotlib.use("Agg")  # REQUIRED for Airflow (no GUI)

import matplotlib.pyplot as plt

from dotenv import load_dotenv


# -----------------------------
# DAG CONFIG
# -----------------------------

load_dotenv("/home/kiwilytics/airflow/dags/jewelry_pipeline/.env")

DAG_ID = "jewelry_simple_pipeline"

#RAW_FILE = "/home/kiwilytics/airflow/Jewelry Transactions Dataset/Data/Raw/jewelry.csv"
RAW_FILE = os.getenv("RAW_FILE")

CLEAN_FILE = os.getenv("CLEAN_FILE")

PG_CONN_ID = os.getenv("PG_CONN_ID")

default_args = {
    "owner": "kiwilytics",
    "start_date": datetime(2026, 2, 3),
    "retries": 1,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
)

# -----------------------------
# 1️⃣ CLEAN DATA
# -----------------------------

def clean_data():
    print("RAW_FILE path:", RAW_FILE)
    print("File exists:", os.path.exists(RAW_FILE))

    # 🔥 Try multiple encodings & separators safely
    try:
        df = pd.read_csv(
            RAW_FILE,
            sep=None,              # auto-detect delimiter
            engine="python",       # required for sep=None
            encoding="utf-8-sig"   # handles BOM safely
        )
    except Exception as e:
        raise RuntimeError(f"CSV READ FAILED: {e}")

    print("==== DEBUG INFO ====")
    print("Shape:", df.shape)
    print("Columns raw:", df.columns.tolist())
    print("First row:", df.head(1).to_dict())
    print("====================")

    # 🚨 HARD FAIL if columns not split
    if len(df.columns) == 1:
        raise ValueError(
            "CSV NOT PARSED CORRECTLY — delimiter/encoding issue. "
            "Header read as a single column."
        )

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)
    )

    required_cols = {
        "date_time",
        "price_in_usd",
        "quantity_of_sku_in_order"
    }

    if not required_cols.issubset(df.columns):
        raise ValueError(
            f"Required columns missing. Found: {df.columns.tolist()}"
        )

    # Minimal cleaning to confirm success
    df["price_in_usd"] = pd.to_numeric(df["price_in_usd"], errors="coerce")
    df["quantity_of_sku_in_order"] = pd.to_numeric(
        df["quantity_of_sku_in_order"], errors="coerce"
    )

    df = df[df["price_in_usd"] > 0]
    df = df[df["quantity_of_sku_in_order"].notnull()]

    df["date_time"] = pd.to_datetime(
        df["date_time"], errors="coerce"
    )
    df = df[df["date_time"].notnull()]

    df["total_amount"] = (
        df["price_in_usd"] * df["quantity_of_sku_in_order"]
    )

    os.makedirs(os.path.dirname(CLEAN_FILE), exist_ok=True)
    df.to_csv(CLEAN_FILE, index=False)

    print("✅ CLEAN TASK COMPLETED")

# -----------------------------
# 2️⃣ LOAD CLEAN DATA
# -----------------------------
def load_clean_data():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    hook.run("CREATE SCHEMA IF NOT EXISTS jewelry;")

    hook.run("""
        DROP TABLE IF EXISTS jewelry.sales;
        CREATE TABLE jewelry.sales (
            date_time TIMESTAMPTZ,
            order_id TEXT,
            product_id TEXT,
            quantity_of_sku_in_order INTEGER,
            category_id TEXT,
            category_alias TEXT,
            brand_id TEXT,
            price_in_usd DOUBLE PRECISION,
            user_id TEXT,
            product_gender TEXT,
            main_color TEXT,
            main_metal TEXT,
            main_gem TEXT,
            total_amount DOUBLE PRECISION
        );
    """)

    hook.copy_expert(
        sql="""
        COPY jewelry.sales
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            QUOTE '"',
            ESCAPE '"'
        );
        """,
        filename=CLEAN_FILE
    )

    print("✅ Clean data loaded to Postgres")

# -----------------------------
# 3️⃣ ANALYTICS
# -----------------------------
def create_analytics():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")

    # -----------------------------
    # CREATE ANALYTICS TABLES
    # -----------------------------
    hook.run("""
        DROP TABLE IF EXISTS analytics.daily_revenue;
        CREATE TABLE analytics.daily_revenue AS
        SELECT
            DATE(date_time) AS order_date,
            SUM(total_amount) AS daily_revenue
        FROM jewelry.sales
        GROUP BY DATE(date_time)
        ORDER BY order_date;
    """)

    # 🔧 FIXED: Use COALESCE to replace NULL category_alias with 'Unknown'
    hook.run("""
        DROP TABLE IF EXISTS analytics.top_categories;
        CREATE TABLE analytics.top_categories AS
        SELECT
            COALESCE(category_alias, 'Unknown') AS category_alias,
            SUM(total_amount) AS total_revenue
        FROM jewelry.sales
        GROUP BY category_alias
        ORDER BY total_revenue DESC;
    """)

    hook.run("""
        DROP TABLE IF EXISTS analytics.top_customers;
        CREATE TABLE analytics.top_customers AS
        SELECT
            user_id,
            SUM(total_amount) AS total_spent
        FROM jewelry.sales
        GROUP BY user_id
        ORDER BY total_spent DESC;
    """)

    # -----------------------------
    # LOAD DATA INTO PANDAS
    # -----------------------------
    daily_df = hook.get_pandas_df("""
        SELECT order_date, daily_revenue
        FROM analytics.daily_revenue;
    """)

    category_df = hook.get_pandas_df("""
        SELECT category_alias, total_revenue
        FROM analytics.top_categories
        LIMIT 10;
    """)

    customer_df = hook.get_pandas_df("""
        SELECT user_id, total_spent
        FROM analytics.top_customers
        LIMIT 10;
    """)

    # Ensure output directory
    output_dir = os.getenv("OUTPUT_DIR")
    os.makedirs(output_dir, exist_ok=True)

    # -----------------------------
    # 📊 DAILY REVENUE LINE CHART
    # -----------------------------
    plt.figure(figsize=(10, 5))
    plt.plot(daily_df["order_date"], daily_df["daily_revenue"])
    plt.title("Daily Revenue")
    plt.xlabel("Date")
    plt.ylabel("Revenue (USD)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/daily_revenue.png")
    plt.close()

    # -----------------------------
    # 📊 TOP CATEGORIES BAR CHART
    # -----------------------------
    plt.figure(figsize=(10, 5))
    plt.bar(category_df["category_alias"], category_df["total_revenue"])
    plt.title("Top 10 Categories by Revenue")
    plt.xlabel("Category")
    plt.ylabel("Revenue (USD)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/top_categories.png")
    plt.close()

    # -----------------------------
    # 📊 TOP CUSTOMERS BAR CHART
    # -----------------------------
    plt.figure(figsize=(10, 5))
    plt.bar(customer_df["user_id"].astype(str), customer_df["total_spent"])
    plt.title("Top 10 Customers by Spend")
    plt.xlabel("User ID")
    plt.ylabel("Total Spent (USD)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/top_customers.png")
    plt.close()

    print("✅ Analytics tables created")
    print("📊 Charts saved to:", output_dir)

# -----------------------------
# TASKS
# -----------------------------
clean_task = PythonOperator(
    task_id="clean_data",
    python_callable=clean_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_clean_data",
    python_callable=load_clean_data,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id="create_analytics",
    python_callable=create_analytics,
    dag=dag,
)

clean_task >> load_task >> analytics_task