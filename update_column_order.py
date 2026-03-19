"""
update_column_order.py
======================
Scan all VNA*.csv files in year/month/day subdirectories of E:\\DataMesRevert
and normalise every file so it has exactly these 24 columns in this order:

    BucCoverQR, BacketBarCode, BendingDistanceValue, PressureTime,
    Temp 1, Temp 2, Temp 3, Temp 4,
    U1, U2, U3, L1, L2, L3, D1, D2, D3, R1, R2, R3,
    Result, Remark, Date, Time

Known source variations handled automatically:
  • "Temp1"→"Temp 1", "Temp2"→"Temp 2", etc. (no space → with space)
  • "Results" → "Result"
  • Missing "Remark" column  → added with empty value
  • Columns in wrong order   → reordered to target
  • Extra columns            → removed
"""

import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BASE_FOLDER = r"E:\DataMesRevert"
LOG_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "update_column_order.log")

# The exact target column order (24 columns)
TARGET_COLUMNS = [
    "BucCoverQR", "BacketBarCode", "BendingDistanceValue", "PressureTime",
    "Temp 1", "Temp 2", "Temp 3", "Temp 4",
    "U1", "U2", "U3",
    "L1", "L2", "L3",
    "D1", "D2", "D3",
    "R1", "R2", "R3",
    "Result", "Remark",
    "Date", "Time",
]

# Map known non-standard header names → standard names (lowercase key)
COLUMN_RENAME_MAP = {
    "temp1":   "Temp 1",
    "temp2":   "Temp 2",
    "temp3":   "Temp 3",
    "temp4":   "Temp 4",
    "results": "Result",
}

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def should_process(file_path: Path) -> bool:
    """Return True only for .csv files whose name starts with 'VNA' (case-insensitive)."""
    return file_path.stem.upper().startswith("VNA")


def find_csv_files(base: str) -> list[Path]:
    """Recursively find all VNA*.csv files inside base_folder."""
    return sorted(f for f in Path(base).rglob("*.csv") if should_process(f))


def _normalise_header(raw_name: str) -> str:
    """
    Return the canonical column name for a raw header field.
    Applies rename rules first, otherwise returns the stripped original.
    """
    stripped = raw_name.strip()
    key = stripped.lower()
    return COLUMN_RENAME_MAP.get(key, stripped)


def process_file(file_path: Path) -> bool:
    """
    Read a CSV, normalise column names, reorder/add/drop columns to match
    TARGET_COLUMNS, then overwrite the file.
    Returns True if the file was modified.
    """
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        if not lines:
            logger.info("    Empty file – skipped")
            return False

        # ── Parse & normalise header ──
        header_line = lines[0].rstrip("\n").rstrip("\r")
        raw_fields = header_line.split(",")
        norm_fields = [_normalise_header(c) for c in raw_fields]

        # Build a lookup: normalised name → column index in source
        col_index: dict[str, int] = {}
        for i, name in enumerate(norm_fields):
            # first occurrence wins (safety against duplicates)
            if name not in col_index:
                col_index[name] = i

        # Quick check: is the file already perfect?
        if norm_fields == TARGET_COLUMNS:
            return False  # nothing to do

        # Make sure we have at least the minimum required columns
        # (we allow "Remark" to be missing – it will be filled with "")
        target_set = set(TARGET_COLUMNS)
        available  = set(col_index.keys())
        missing    = target_set - available
        # "Remark" can be missing (old Jan-2025 format); anything else is an error
        unexpected_missing = missing - {"Remark"}
        if unexpected_missing:
            logger.warning(f"    Missing columns {unexpected_missing} – skipped")
            return False

        extra = available - target_set
        if extra:
            logger.info(f"    Dropping extra columns: {extra}")

        # ── Rebuild every line ──
        new_lines: list[str] = [",".join(TARGET_COLUMNS) + "\n"]

        for line in lines[1:]:
            stripped = line.rstrip("\n").rstrip("\r")
            if not stripped:
                new_lines.append("\n")
                continue

            values = stripped.split(",")
            # Pad short rows so index lookups don't crash
            while len(values) < len(raw_fields):
                values.append("")

            new_values: list[str] = []
            for col_name in TARGET_COLUMNS:
                idx = col_index.get(col_name)
                if idx is not None and idx < len(values):
                    new_values.append(values[idx])
                else:
                    # Column didn't exist in source (e.g. "Remark") → empty
                    new_values.append("")

            new_lines.append(",".join(new_values) + "\n")

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.writelines(new_lines)

        return True

    except Exception as exc:
        logger.error(f"    ✘ Error: {exc}")
        return False


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    # Windows compatibility for multiprocessing
    multiprocessing.freeze_support()

    logger.info("=" * 60)
    logger.info(f"Base folder    : {BASE_FOLDER}")
    logger.info(f"Target columns : {len(TARGET_COLUMNS)} cols")
    logger.info(f"  {TARGET_COLUMNS}")
    logger.info(f"File filter    : VNA*.csv only")
    logger.info("=" * 60)

    if not os.path.isdir(BASE_FOLDER):
        logger.error(f"Base folder does not exist: {BASE_FOLDER}")
        return

    csv_files = find_csv_files(BASE_FOLDER)
    total = len(csv_files)

    if total == 0:
        logger.warning("No matching CSV files found.")
        return

    logger.info(f"Found {total:,} CSV file(s). Starting Multiprocessing (All Cores)...\n")

    updated = 0
    skipped = 0
    errors  = 0

    # Determine number of workers (leave 1 core free for OS)
    num_workers = max(1, multiprocessing.cpu_count() - 1)
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_file, f): f for f in csv_files}
        
        for idx, future in enumerate(as_completed(future_to_file), start=1):
            if idx % 10000 == 0 or idx == 1:
                logger.info(f"Progress: {idx:,}/{total:,}  (current_updated~{updated:,})")
            
            try:
                changed = future.result()
                if changed:
                    updated += 1
                else:
                    skipped += 1
            except Exception as exc:
                file_path = future_to_file[future]
                logger.error(f"Error processing {file_path}: {exc}")
                errors += 1

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Done!  Updated: {updated:,}  |  Skipped: {skipped:,}  |  Errors: {errors:,}")
    logger.info(f"Log saved to: {LOG_FILE}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
