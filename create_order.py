from sqlalchemy import text
from datetime import datetime
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # your decorator

@debug_log
def create_order(product_id: int, quantity: int) -> dict:
    try:
        # Step 1: Read product price from DuckDB
        with get_duckdb_conn() as duck_con:
            product_query = "SELECT price FROM product WHERE product_id = ?"
            result = duck_con.execute(product_query, (product_id,)).fetchone()

            if not result:
                return {
                    "type": "error",
                    "action": "create_order",
                    "status": "failed",
                    "message": f"Product ID {product_id} not found."
                }

            price = result[0]
            revenue = round(price * quantity, 2)
            sale_date = datetime.now().strftime("%Y-%m-%d")
            order_status = "Open"

            # Step 2: Insert into sales table (SQLite)
            with sqlite_engine.begin() as sqlite_conn:
                insert_sale = text("""
                    INSERT INTO sales (product_id, quantity, sale_date, revenue, order_status)
                    VALUES (:product_id, :quantity, :sale_date, :revenue, :order_status)
                """)
                result = sqlite_conn.execute(insert_sale, {
                    "product_id": product_id,
                    "quantity": quantity,
                    "sale_date": sale_date,
                    "revenue": revenue,
                    "order_status": order_status
                })
                sale_id = result.lastrowid

            # Step 3: Update inventory in DuckDB
            inv_query = "SELECT total_qty, committed_qty FROM inventory WHERE product_id = ?"
            inv_result = duck_con.execute(inv_query, (product_id,)).fetchone()

            if not inv_result:
                return {
                    "type": "error",
                    "action": "create_order",
                    "status": "failed",
                    "message": f"Inventory for Product ID {product_id} not found."
                }

            total_qty, committed_qty = inv_result
            new_committed = committed_qty + quantity
            new_available = max(0, total_qty - new_committed)
            backorder_qty = max(0, new_committed - total_qty)

            duck_con.execute("""
                UPDATE inventory
                SET committed_qty = ?, available_qty = ?, backorder_qty = ?
                WHERE product_id = ?
            """, (new_committed, new_available, backorder_qty, product_id))

        # Step 4: Update order status to "Committed" in SQLite
        with sqlite_engine.begin() as sqlite_conn:
            update_status = text("UPDATE sales SET order_status = :status WHERE rowid = :sale_id")
            sqlite_conn.execute(update_status, {"status": "Committed", "sale_id": sale_id})

        return {
            "type": "action",
            "action": "create_order",
            "status": "success",
            "data": {
                "sale_id": sale_id,
                "product_id": product_id,
                "quantity": quantity,
                "revenue": revenue,
                "inventory": {
                    "committed": new_committed,
                    "available": new_available,
                    "backorder": backorder_qty,
                }
            },
            "message": (
                f"✅ Order {sale_id} created → Product {product_id}, Qty: {quantity}, "
                f"Revenue: {revenue}, Committed Qty: {new_committed}, Backorder Qty: {backorder_qty}"
            )
        }

    except Exception as e:
        return {
            "type": "error",
            "action": "create_order",
            "status": "failed",
            "message": f"Order creation failed: {str(e)}"
        }
