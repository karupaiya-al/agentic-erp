from sqlalchemy import create_engine
import duckdb
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
SQLITE_DB_PATH = os.path.join(DATA_DIR, "sales_data.db")
DUCKDB_DB_PATH = os.path.join(DATA_DIR, "retail_data.duckdb")

# Create SQLite engine globally (safe to reuse)
sqlite_engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")

def get_sqlite_connection():
    # Context manager usage recommended
    return sqlite_engine.connect()

def get_duckdb_conn(read_only=False):
    return duckdb.connect(database=DUCKDB_DB_PATH, read_only=read_only)
