[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/IVML3cZy)
# 🏭 Assignment: The "Ghost Shipment" Bug

**Topic:** Database Transactions, Atomicity, and Error Handling (ACID)

**Context:** Industrial Engineering / Manufacturing Execution Systems (MES)

## 1. The Scenario

You have just been hired as a Systems Integrator for a manufacturing plant. The factory uses a custom Python/SQL software called **"Factory Control System v1.0"** to manage inventory on the shop floor.

Workers use this software to move raw materials from the **Warehouse** to the **Assembly Line**.
When a worker clicks "Execute Shipment," the system is supposed to do two things simultaneously:

1. **Deduct** the material from the `Inventory` table.

2. **Log** the movement in the `Shipment_Log` table for auditing.

## 2. The Problem

The previous developer left a critical bug in the transaction logic.

If a worker tries to move more items than are currently in stock (e.g., ordering 10 units when only 5 are available):

- **Step 1 Fails:** The database correctly blocks the inventory deduction (because stock cannot be negative).

- **Step 2 Succeeds:** The system *still* inserts a record into the log saying "10 units moved."

**This creates a "Ghost Shipment."** The logs say material was moved to the assembly line, but the inventory never left the warehouse. This causes massive confusion and production halts.

## 3. Your Task

Your job is to fix the `process_shipment` function in `processor.py`.

You must implement **Database Atomicity** principles to ensure that **either BOTH steps happen, or NEITHER happens.**

### Instructions

1. **Install Dependencies:**
   
   Bash
   
   ```
   pip install -r requirements.txt
   ```

2. **Run the Application:**
   
   Bash
   
   ```
   python main.py
   ```

3. **Reproduce the Bug:**
   
   - Select "Titanium Alloy Sheets" (Current Stock: 5).
   
   - Set Quantity to **10**.
   
   - Click "EXECUTE SHIPMENT".
   
   - **Observe:** The Console shows "Step 1 Failed" but "Step 2 Success." The "Shipment History Logs" table shows a movement that never happened.

4. **Fix the Code:**
   
   - Open `processor.py`.
   
   - Wrap the transaction logic using `try`, `except`, and `rollback`.
   
   - Ensure that if the Inventory update fails, the Log insert is skipped/undone.
   
   - Ensure that `commit()` is only called if *both* steps succeed.

## 4. Acceptance Criteria

- [ ] When ordering more stock than available, the application displays an error message.

- [ ] **Crucial:** No new rows appear in the "Shipment History Logs" after a failed attempt.

- [ ] Valid orders (ordering 3 units when 5 are available) still work correctly.
