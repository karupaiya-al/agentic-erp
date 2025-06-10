from database.schema_sqlite import create_sqlite_schema
from database.schema_duckdb import create_duckdb_schema
from database.populate_duckdb import populate_duckdb

def setup_all():
    print("Creating SQLite schema...")
    create_sqlite_schema()
    print("Creating DuckDB schema...")
    create_duckdb_schema()
    print("Populating DuckDB data...")
    populate_duckdb()
    print("Setup complete!")

if __name__ == "__main__":
    setup_all()
