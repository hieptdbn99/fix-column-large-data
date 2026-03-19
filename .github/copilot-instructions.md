<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# Project: UpdateDataColumn

This is a Python script project that scans Excel files in a year/month/day folder structure under `E:\DataMesRevert` and reorders columns so that the `Result` column appears immediately before the `Remark` column.

## Key Details
- Main script: `update_column_order.py`
- Libraries used: `pandas`, `openpyxl`
- Column matching is **case-insensitive**
- Supports `.xlsx` and `.xls` files
- A log file `update_column_order.log` is written alongside the script
