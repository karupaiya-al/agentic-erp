from database.db_utils import sqlite_engine
from sqlalchemy import text

def create_sqlite_schema():
    engine = sqlite_engine
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS sales"))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sales (
                sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                sale_date TEXT NOT NULL,
                revenue REAL NOT NULL,
                order_status TEXT NOT NULL
            )
        """))
    print("SQLite schema created.")

if __name__ == "__main__":
    create_sqlite_schema()
