from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # your decorator

@debug_log
def schedule_order(sale_id: int) -> dict:
    try:
        # Step 1: Get sale info from SQLite
        with sqlite_engine.connect() as conn:
            result = conn.execute(
                text("SELECT product_id, quantity, order_status FROM sales WHERE sale_id = :sale_id"),
                {"sale_id": sale_id}
            ).fetchone()

        if not result:
            return {
                "type": "error",
                "action": "schedule_order",
                "status": "failed",
                "message": f"Sale ID {sale_id} not found."
            }

        product_id, sale_qty, status = result

        if status.lower() != "committed":
            return {
                "type": "info",
                "action": "schedule_order",
                "status": "failed",
                "message": f"Sale ID {sale_id} is already '{status}' and cannot be scheduled again."
            }

        # Step 2: Get inventory from DuckDB
        with get_duckdb_conn() as duck_conn:
            inventory = duck_conn.execute(
                "SELECT committed_qty, scheduled_qty FROM inventory WHERE product_id = ?",
                (product_id,)
            ).fetchone()

            if not inventory:
                return {
                    "type": "error",
                    "action": "schedule_order",
                    "status": "failed",
                    "message": f"Inventory record for product {product_id} not found."
                }

            committed_qty, scheduled_qty = inventory
            remaining_schedulable = committed_qty - scheduled_qty

            # Step 3: Check availability
            if remaining_schedulable >= sale_qty:
                # Step 4a: Update order status in SQLite
                with sqlite_engine.begin() as conn:
                    conn.execute(
                        text("UPDATE sales SET order_status = 'Scheduled' WHERE sale_id = :sale_id"),
                        {"sale_id": sale_id}
                    )

                # Step 4b: Update inventory in DuckDB
                new_scheduled_qty = scheduled_qty + sale_qty
                duck_conn.execute(
                    """
                    UPDATE inventory
                    SET scheduled_qty = ?
                    WHERE product_id = ?
                    """,
                    (new_scheduled_qty, product_id)
                )

                return {
                    "type": "action",
                    "action": "schedule_order",
                    "status": "success",
                    "data": {
                        "sale_id": sale_id,
                        "product_id": product_id,
                        "scheduled_qty": sale_qty,
                        "remaining_schedulable": remaining_schedulable - sale_qty
                    },
                    "message": (
                        f"✅ Sale {sale_id} scheduled successfully. "
                        f"Scheduled Qty: {sale_qty}, Remaining schedulable: {remaining_schedulable - sale_qty}"
                    )
                }
            else:
                return {
                    "type": "error",
                    "action": "schedule_order",
                    "status": "failed",
                    "message": (
                        f"Insufficient committed quantity to schedule sale {sale_id}. "
                        f"Only {remaining_schedulable} available. Refill backorder to proceed."
                    )
                }
    except Exception as e:
        return {
            "type": "error",
            "action": "schedule_order",
            "status": "failed",
            "message": f"Scheduling failed due to error: {str(e)}"
        }
