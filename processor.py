import sqlite3

class ShipmentProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def process_shipment(self, item_name, quantity, log_callback):
        """
        Executes the shipment logic atomically (all-or-nothing).
        If there isn't enough stock, NOTHING is saved (no ghost shipment log).
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        log_callback(f"--- STARTING TRANSACTION: Move {quantity} of {item_name} ---")

        try:
            # Start transaction explicitly
            conn.execute("BEGIN")

            # STEP 1: Only deduct if enough stock is available
            cursor.execute(
                """
                UPDATE inventory
                SET stock_qty = stock_qty - ?
                WHERE item_name = ? AND stock_qty >= ?
                """,
                (quantity, item_name, quantity),
            )

            # If no row was updated, stock was insufficient (or item not found)
            if cursor.rowcount != 1:
                raise ValueError("Not enough stock for this shipment (or item not found).")

            log_callback(">> STEP 1 SUCCESS: Inventory Deducted.")

            # STEP 2: Log shipment ONLY if step 1 succeeded
            cursor.execute(
                "INSERT INTO shipment_log (item_name, qty_moved) VALUES (?, ?)",
                (item_name, quantity),
            )
            log_callback(">> STEP 2 SUCCESS: Shipment Logged.")

            conn.commit()
            log_callback("--- TRANSACTION COMMITTED ---")

        except Exception as e:
            conn.rollback()
            log_callback(f"--- TRANSACTION ROLLED BACK: {e} ---")

        finally:
            conn.close()
