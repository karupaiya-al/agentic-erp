from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # your decorator

@debug_log
def return_order(sale_id: int) -> dict:
    try:
        # Step 1: Fetch sale details from SQLite
        with sqlite_engine.connect() as sqlite_conn:
            result = sqlite_conn.execute(
                text("SELECT product_id, quantity, order_status FROM sales WHERE sale_id = :sid"),
                {"sid": sale_id}
            ).fetchone()

        if not result:
            return {
                "type": "error",
                "action": "return_order",
                "status": "failed",
                "message": f"Sale ID {sale_id} not found."
            }

        product_id, quantity, status = result

        if status != "Complete":
            return {
                "type": "info",
                "action": "return_order",
                "status": "failed",
                "message": f"Only completed sales can be returned. Current status: {status}"
            }

        # Step 2: Update sale status to 'Returned'
        with sqlite_engine.begin() as sqlite_conn:
            sqlite_conn.execute(
                text("UPDATE sales SET order_status = 'Returned' WHERE sale_id = :sid"),
                {"sid": sale_id}
            )

        # Step 3: Update inventory in DuckDB
        with get_duckdb_conn() as duck_conn:
            inventory = duck_conn.execute(
                "SELECT total_qty, available_qty FROM inventory WHERE product_id = ?",
                (product_id,)
            ).fetchone()

            if not inventory:
                return {
                    "type": "error",
                    "action": "return_order",
                    "status": "failed",
                    "message": f"Inventory record not found for product {product_id}"
                }

            total_qty, available_qty = inventory
            updated_total = total_qty + quantity
            updated_available = available_qty + quantity

            duck_conn.execute("""
                UPDATE inventory
                SET total_qty = ?, available_qty = ?
                WHERE product_id = ?
            """, (updated_total, updated_available, product_id))

        return {
            "type": "action",
            "action": "return_order",
            "status": "success",
            "data": {
                "sale_id": sale_id,
                "product_id": product_id,
                "quantity_returned": quantity,
                "inventory": {
                    "total_qty": updated_total,
                    "available_qty": updated_available
                }
            },
            "message": (
                f"✅ Sale ID {sale_id} marked as Returned.\n"
                f"Inventory updated: Total = {updated_total}, Available = {updated_available}"
            )
        }

    except Exception as e:
        return {
            "type": "error",
            "action": "return_order",
            "status": "failed",
            "message": f"Error processing return for Sale ID {sale_id}: {str(e)}"
        }
