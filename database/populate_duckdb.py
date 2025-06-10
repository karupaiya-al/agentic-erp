from database.db_utils import get_duckdb_conn

def populate_duckdb():
    with get_duckdb_conn(read_only=False) as conn:
        conn.execute("""
            INSERT INTO product (product_id, name, category, status, price) VALUES
            (101, 'Laptop', 'Electronics','Active', 1000.0),
            (102, 'Phone', 'Electronics','Active', 500.0),
            (103, 'Tablet','Electronics', 'Active', 750.0)
        """)
        conn.execute("""
            INSERT INTO inventory (product_id, total_qty, committed_qty, available_qty, backorder_qty, scheduled_qty) VALUES
            (101, 10, 0, 0, 10, 0),
            (102, 10, 0, 0, 10, 0),
            (103, 10, 0, 0, 10, 0)
        """)
    print("DuckDB data populated.")

if __name__ == "__main__":
    populate_duckdb()
