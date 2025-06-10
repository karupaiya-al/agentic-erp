from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # assuming your decorator is here

@debug_log
def cancel_order(sale_id: int) -> dict:
    try:
        # Step 1: Get sale record from SQLite
        with sqlite_engine.connect() as conn:
            result = conn.execute(
                text("SELECT product_id, quantity, order_status FROM sales WHERE sale_id = :sale_id"),
                {"sale_id": sale_id}
            ).fetchone()

        if not result:
            return {
                "type": "error",
                "action": "cancel_order",
                "status": "failed",
                "message": f"Sale ID {sale_id} not found."
            }

        product_id, quantity, current_status = result

        if current_status == "Cancel":
            return {
                "type": "action",
                "action": "cancel_order",
                "status": "success",
                "data": {
                    "sale_id": sale_id,
                    "product_id": product_id,
                    "status": "already_cancelled"
                },
                "message": f"ℹ️ Sale ID {sale_id} is already cancelled."
            }

        if current_status == "Complete":
            return {
                "type": "action",
                "action": "cancel_order",
                "status": "failed",
                "data": {
                    "sale_id": sale_id,
                    "product_id": product_id,
                    "status": "completed"
                },
                "message": f"❌ Sale ID {sale_id} is completed and cannot be cancelled."
            }

        # Step 2: Update sale status to 'Cancel'
        with sqlite_engine.begin() as conn:
            conn.execute(
                text("UPDATE sales SET order_status = :status WHERE sale_id = :sale_id"),
                {"status": "Cancel", "sale_id": sale_id}
            )

        # Step 3: Reverse inventory allocations in DuckDB
        with get_duckdb_conn() as duck_conn:
            inventory = duck_conn.execute(
                "SELECT committed_qty, scheduled_qty, available_qty, backorder_qty FROM inventory WHERE product_id = ?",
                [product_id]
            ).fetchone()

            if not inventory:
                return {
                    "type": "error",
                    "action": "cancel_order",
                    "status": "failed",
                    "message": f"Inventory for Product ID {product_id} not found."
                }

            committed_qty, scheduled_qty, available_qty, backorder_qty = inventory

            adjusted_committed = max(0, committed_qty - quantity)
            adjusted_scheduled = max(0, scheduled_qty - quantity)
            adjusted_available = available_qty + quantity
            adjusted_backorder = max(0, backorder_qty - quantity)

            duck_conn.execute("""
                UPDATE inventory
                SET committed_qty = ?, scheduled_qty = ?, available_qty = ?, backorder_qty = ?
                WHERE product_id = ?
            """, (
                adjusted_committed,
                adjusted_scheduled,
                adjusted_available,
                adjusted_backorder,
                product_id
            ))

        return {
            "type": "action",
            "action": "cancel_order",
            "status": "success",
            "data": {
                "sale_id": sale_id,
                "product_id": product_id,
                "cancelled_qty": quantity,
                "inventory": {
                    "available": adjusted_available,
                    "committed": adjusted_committed,
                    "scheduled": adjusted_scheduled,
                    "backorder": adjusted_backorder
                }
            },
            "message": (
                f"✅ Sale ID {sale_id} cancelled. Inventory updated: "
                f"Available = {adjusted_available}, Committed = {adjusted_committed}, "
                f"Scheduled = {adjusted_scheduled}, Backorder = {adjusted_backorder}"
            )
        }

    except Exception as e:
        return {
            "type": "error",
            "action": "cancel_order",
            "status": "failed",
            "message": f"Error cancelling order {sale_id}: {str(e)}"
        }
