# Data-Pipeline-Development-
import pandas as pd
import requests
import sqlite3

# Step 1: Extract
def extract_from_api(url: str) -> pd.DataFrame:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data)

# Step 2: Transform
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Example cleaning
    df = df.dropna()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["value"] = df["value"].astype(float)
    return df

# Step 3: Load
def load_to_sqlite(df: pd.DataFrame, db_name="pipeline.db", table_name="processed_data"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

# Orchestrate pipeline
def run_pipeline():
    print("Extracting...")
    df = extract_from_api("https://api.example.com/data")

    print("Transforming...")
    df = transform_data(df)

    print("Loading...")
    load_to_sqlite(df)

    print("Pipeline completed ✅")

if __name__ == "__main__":
    run_pipeline()
