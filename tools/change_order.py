from sqlalchemy import text
from database.db_utils import get_duckdb_conn, sqlite_engine
from tools.debug_logger import debug_log  # Importing the decorator

@debug_log
def change_order(sale_id: int, new_quantity: int) -> dict:
    # Step 1: Fetch sale record from SQLite
    with sqlite_engine.connect() as sqlite_conn:
        sale_query = text("""
            SELECT product_id, quantity, revenue, order_status 
            FROM sales 
            WHERE sale_id = :sid
        """)
        result = sqlite_conn.execute(sale_query, {"sid": sale_id}).fetchone()

    if not result:
        return {
            "type": "action",
            "action": "change_order",
            "status": "failed",
            "message": f"❌ Sale ID {sale_id} not found."
        }

    product_id, old_qty, old_revenue, status = result

    if status not in ["Open", "Committed"]:
        return {
            "type": "action",
            "action": "change_order",
            "status": "failed",
            "message": f"ℹ️ Only 'Open' or 'Committed' orders can be modified. Current status: {status}"
        }

    # Step 2: Fetch product price from DuckDB
    with get_duckdb_conn() as duck_conn:
        price_result = duck_conn.execute(
            "SELECT price FROM product WHERE product_id = ?", 
            (product_id,)
        ).fetchone()

        if not price_result:
            return {
                "type": "action",
                "action": "change_order",
                "status": "failed",
                "message": f"❌ Price info not found for product {product_id}"
            }

        price = price_result[0]
        new_revenue = round(price * new_quantity, 2)

    # Step 3: Update order in SQLite
    with sqlite_engine.begin() as conn:
        update_query = text("""
            UPDATE sales
            SET quantity = :qty, revenue = :rev
            WHERE sale_id = :sid
        """)
        conn.execute(update_query, {
            "qty": new_quantity,
            "rev": new_revenue,
            "sid": sale_id
        })

    # Step 4: Adjust inventory commitment in DuckDB
    delta_qty = new_quantity - old_qty
    with get_duckdb_conn() as duck_conn:
        inventory = duck_conn.execute(
            "SELECT committed_qty, backorder_qty, available_qty FROM inventory WHERE product_id = ?",
            (product_id,)
        ).fetchone()

        if not inventory:
            return {
                "type": "action",
                "action": "change_order",
                "status": "failed",
                "message": f"❌ No inventory entry for product {product_id}"
            }

        committed, backorder, available = inventory
        new_committed = committed + delta_qty
        new_available = max(0, available - delta_qty)
        new_backorder = max(0, new_committed - new_available)

        duck_conn.execute("""
            UPDATE inventory
            SET committed_qty = ?, available_qty = ?, backorder_qty = ?
            WHERE product_id = ?
        """, (new_committed, new_available, new_backorder, product_id))

    # Return structured success response
    return {
        "type": "action",
        "action": "change_order",
        "status": "success",
        "sale_id": sale_id,
        "product_id": product_id,
        "new_quantity": new_quantity,
        "new_revenue": new_revenue,
        "inventory_update": {
            "committed_qty": new_committed,
            "available_qty": new_available,
            "backorder_qty": new_backorder
        },
        "message": (
            f"✅ Order {sale_id} updated. New quantity: {new_quantity}, "
            f"Revenue: {new_revenue}, Committed: {new_committed}, Available: {new_available}, "
            f"Backorder: {new_backorder}"
        )
    }
