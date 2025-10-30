# load_data.py
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
INPUT_FILE = "transformed_high_popularity_movies.csv"
TARGET_TABLE = "high_popularity_movies"

# --- Database Connection Details ---
# Read from environment variables (DB_HOST will be overridden by docker-compose)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database Connection Error: {e}")
        return None

def create_table(conn, table_name):
    """Checks for and creates the target table if it does not exist."""
    print(f"Checking for and creating table: {table_name}")
    try:
        with conn.cursor() as cur:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                release_date DATE,
                popularity REAL,
                vote_average REAL,
                overview TEXT
            );
            """
            cur.execute(create_table_query)
        conn.commit()
        print("Table check/creation complete.")
    except psycopg2.Error as e:
        print(f"Database Error during table creation: {e}")
        conn.rollback()
        raise

def load_data_into_postgres(input_file, table_name):
    """Loads data from a CSV file into the PostgreSQL table."""
    
    if not os.path.exists(input_file):
        print(f"Error: Input CSV file not found: {input_file}")
        return

    conn = get_db_connection()
    if conn is None:
        return

    try:
        # 1. Create table if necessary
        create_table(conn, table_name)

        # 2. Clear existing data (Snapshot approach)
        print(f"Clearing existing data from {table_name}...")
        with conn.cursor() as cur:
            # TRUNCATE is fast and perfect for ETL snapshots
            cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
            conn.commit() # CRITICAL: Commit the TRUNCATE before loading!
        print("Existing data cleared.")

        # 3. Load data from CSV
        df = pd.read_csv(input_file)
        df = df.where(pd.notnull(df), None) # Convert NaNs to None for SQL

        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]
        
        insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
        """
        
        print(f"Attempting to load {len(df)} records into {table_name}...")
        with conn.cursor() as cur:
            # Use execute_values for efficient bulk insertion
            execute_values(cur, insert_query, values, page_size=100)
        
        conn.commit()
        print(f"--- Data Loading (L) complete! {len(df)} records loaded successfully. ---")

    except psycopg2.Error as e:
        print(f"Database Error: Could not connect or insert data. Check credentials and server status.")
        print(f"Original DB Error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("--- Starting Data Loading (L) into PostgreSQL ---")
    load_data_into_postgres(INPUT_FILE, TARGET_TABLE)