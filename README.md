# UpdateDataColumn

A Python utility that **scans all `VNA*.csv` files** inside the `E:\DataMesRevert` folder (organised in `Year\Month\Day` sub-directories) and **normalises every file to a standard 24-column structure**.

---

## Target Column Order (24 columns)

```
BucCoverQR, BacketBarCode, BendingDistanceValue, PressureTime,
Temp 1, Temp 2, Temp 3, Temp 4,
U1, U2, U3, L1, L2, L3, D1, D2, D3, R1, R2, R3,
Result, Remark, Date, Time
```

---

## Requirements

- **Python 3.10+** (no external libraries needed – uses only the standard library)

---

## Usage

```bash
python update_column_order.py
```

The script will:

1. Recursively walk `E:\DataMesRevert` and find all `VNA*.csv` files.
2. Normalise column names (`Temp1` → `Temp 1`, `Results` → `Result`).
3. Reorder columns to match the target structure above.
4. Add missing columns (e.g. `Remark`) with empty values.
5. Remove any extra columns not in the target list.
6. **Overwrite** the original file and write a log to `update_column_order.log`.

---

## Known Source Variations

| Period | Differences |
|--------|-------------|
| 2025/01 | `Temp1`–`Temp4` (no space), `Results` instead of `Result`, no `Remark` column |
| 2025/02–04 | Already correct |
| 2025/05–2026+ | `Result` in wrong position, column order `L,R,U,D` instead of `U,L,D,R` |

---

## Configuration

Edit the constants near the top of `update_column_order.py`:

| Variable         | Default            | Description                     |
|------------------|--------------------|---------------------------------|
| `BASE_FOLDER`    | `E:\DataMesRevert` | Root folder to scan             |
| `TARGET_COLUMNS` | *(24 columns)*     | The exact target column order   |
| `COLUMN_RENAME_MAP` | *(see script)*  | Maps old names → standard names |

---

## Log

A log file named `update_column_order.log` is created in the same directory as the script.  
It records progress every 10,000 files, warnings, errors, and a final summary.

---

## Notes

- Only `VNA*.csv` files are processed; `YYYY-MM-DD.xlsx` files are ignored.
- Column name matching is **case-insensitive**.
- The original file is **overwritten in place** – make sure you have a backup before running.
- Files already in the correct format are **skipped** (no write).
