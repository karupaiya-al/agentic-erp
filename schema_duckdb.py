from database.db_utils import get_duckdb_conn

def create_duckdb_schema():
    with get_duckdb_conn(read_only=False) as conn:
        conn.execute("DROP TABLE IF EXISTS product")
        conn.execute("DROP TABLE IF EXISTS inventory")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            name VARCHAR,
            category VARCHAR,
            status VARCHAR, 
            price DOUBLE
        )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                product_id INTEGER PRIMARY KEY,
                total_qty INTEGER,
                committed_qty INTEGER,
                available_qty INTEGER,
                backorder_qty INTEGER,
                scheduled_qty INTEGER
            )
        """)
    print("DuckDB schema created.")

if __name__ == "__main__":
    create_duckdb_schema()
