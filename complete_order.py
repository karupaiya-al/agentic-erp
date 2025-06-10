from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # your decorator

@debug_log
def complete_order(sale_id: int) -> dict:
    try:
        # Step 1: Get sale record from SQLite
        with sqlite_engine.connect() as conn:
            sale_query = text("""
                SELECT product_id, quantity, order_status 
                FROM sales 
                WHERE sale_id = :sale_id
            """)
            result = conn.execute(sale_query, {"sale_id": sale_id}).fetchone()

        if not result:
            return {
                "type": "error",
                "action": "complete_order",
                "status": "failed",
                "message": f"Sale ID {sale_id} not found."
            }

        product_id, quantity, current_status = result

        if current_status.lower() == "complete":
            return {
                "type": "action",
                "action": "complete_order",
                "status": "success",
                "data": {
                    "sale_id": sale_id,
                    "product_id": product_id,
                    "status": "already_complete"
                },
                "message": f"ℹ️ Sale ID {sale_id} is already marked as Complete."
            }

        # Step 2: Update sale status to 'Complete' in SQLite
        with sqlite_engine.begin() as conn:
            update_query = text("""
                UPDATE sales 
                SET order_status = 'Complete' 
                WHERE sale_id = :sale_id
            """)
            conn.execute(update_query, {"sale_id": sale_id})

        # Step 3: Update inventory in DuckDB
        with get_duckdb_conn() as duck_conn:
            inventory_query = """
                SELECT total_qty, committed_qty, scheduled_qty 
                FROM inventory 
                WHERE product_id = ?
            """
            inventory = duck_conn.execute(inventory_query, [product_id]).fetchone()

            if not inventory:
                return {
                    "type": "error",
                    "action": "complete_order",
                    "status": "failed",
                    "message": f"Inventory for Product ID {product_id} not found."
                }

            total_qty, committed_qty, scheduled_qty = inventory

            # Safely compute new values
            new_total = max(0, total_qty - quantity)
            new_committed = max(0, committed_qty - quantity)
            new_scheduled = max(0, scheduled_qty - quantity)

            duck_conn.execute("""
                UPDATE inventory
                SET total_qty = ?, committed_qty = ?, scheduled_qty = ?
                WHERE product_id = ?
            """, (new_total, new_committed, new_scheduled, product_id))

        return {
            "type": "action",
            "action": "complete_order",
            "status": "success",
            "data": {
                "sale_id": sale_id,
                "product_id": product_id,
                "inventory": {
                    "total": new_total,
                    "committed": new_committed,
                    "scheduled": new_scheduled
                }
            },
            "message": (
                f"✅ Sale ID {sale_id} marked as Complete. "
                f"Inventory updated: Total = {new_total}, Committed = {new_committed}, Scheduled = {new_scheduled}"
            )
        }

    except Exception as e:
        return {
            "type": "error",
            "action": "complete_order",
            "status": "failed",
            "message": f"Completion failed due to error: {str(e)}"
        }
