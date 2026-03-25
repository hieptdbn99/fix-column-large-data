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
import sys
import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BASE_FOLDER = r"E:\mesv4"
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
# WORKER FUNCTION (runs in child processes)
# NOTE: No logging here – workers return results back to main process.
#       This prevents FileHandler conflicts on Windows spawn.
# ─────────────────────────────────────────────

def _normalise_header(raw_name: str) -> str:
    stripped = raw_name.strip()
    return COLUMN_RENAME_MAP.get(stripped.lower(), stripped)


def process_file(file_path_str: str) -> tuple[bool, str | None]:
    """
    Read a CSV, normalise columns, overwrite the file.
    Returns (changed: bool, error_or_warning: str | None).
    Worker processes MUST NOT call any module-level logger.
    """
    file_path = Path(file_path_str)
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        if not lines:
            return False, None

        header_line = lines[0].rstrip("\r\n")
        raw_fields  = header_line.split(",")
        norm_fields = [_normalise_header(c) for c in raw_fields]

        col_index: dict[str, int] = {}
        for i, name in enumerate(norm_fields):
            if name not in col_index:
                col_index[name] = i

        if norm_fields == TARGET_COLUMNS:
            return False, None

        target_set = set(TARGET_COLUMNS)
        missing    = target_set - set(col_index)
        unexpected_missing = missing - {"Remark"}
        if unexpected_missing:
            return False, f"SKIP missing cols {unexpected_missing}: {file_path.name}"

        new_lines: list[str] = [",".join(TARGET_COLUMNS) + "\n"]

        for line in lines[1:]:
            stripped = line.rstrip("\r\n")
            if not stripped:
                new_lines.append("\n")
                continue

            values = stripped.split(",")
            while len(values) < len(raw_fields):
                values.append("")

            new_values: list[str] = []
            for col_name in TARGET_COLUMNS:
                idx = col_index.get(col_name)
                new_values.append(values[idx] if idx is not None and idx < len(values) else "")

            new_lines.append(",".join(new_values) + "\n")

        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.writelines(new_lines)

        return True, None

    except Exception as exc:
        return False, f"ERROR {file_path.name}: {exc}"


# ─────────────────────────────────────────────
# MAIN (logging lives ONLY here)
# ─────────────────────────────────────────────

def main():
    multiprocessing.freeze_support()

    # ── Setup logging in main process only ──
    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console: show INFO so user can see progress in terminal
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("=" * 60)
    logger.info(f"Base folder : {BASE_FOLDER}")
    logger.info(f"Log file    : {LOG_FILE}")
    logger.info("=" * 60)

    if not os.path.isdir(BASE_FOLDER):
        logger.error(f"Folder not found: {BASE_FOLDER}")
        print(f"ERROR: Folder not found: {BASE_FOLDER}", file=sys.stderr)
        return

    logger.info("Scanning files, please wait...")
    csv_files = [
        str(f) for f in Path(BASE_FOLDER).rglob("*.csv")
        if f.stem.upper().startswith("VNA")
    ]
    total = len(csv_files)

    if total == 0:
        logger.warning("No matching VNA*.csv files found.")
        return

    # ThreadPoolExecutor is MUCH faster than ProcessPoolExecutor for I/O-bound tasks:
    # - No spawn overhead (Windows multiprocessing creates new Python interpreters)
    # - No serialization overhead for passing data between processes
    # - Threads share memory, no 1.5M Future objects bloating RAM
    num_workers = min(32, max(4, multiprocessing.cpu_count() * 2))
    logger.info(f"Found {total:,} VNA*.csv file(s). Workers: {num_workers} (ThreadPool)")

    updated = 0
    skipped = 0
    errors  = 0
    start_time = time.time()
    LOG_INTERVAL = 10_000  # log every N files
    BATCH_SIZE   = 50_000  # submit in batches to limit memory

    for batch_start in range(0, total, BATCH_SIZE):
        batch = csv_files[batch_start : batch_start + BATCH_SIZE]

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_path = {executor.submit(process_file, f): f for f in batch}

            for future in as_completed(future_to_path):
                changed, msg = future.result()
                if msg:
                    if msg.startswith("ERROR"):
                        logger.error(msg)
                        errors += 1
                    else:
                        logger.warning(msg)
                        skipped += 1
                elif changed:
                    updated += 1
                else:
                    skipped += 1

                idx = updated + skipped + errors
                # Print progress every LOG_INTERVAL files
                if idx % LOG_INTERVAL == 0 or idx == total:
                    elapsed = time.time() - start_time
                    rate = idx / elapsed if elapsed > 0 else 0
                    pct = idx / total * 100
                    remaining = (total - idx) / rate if rate > 0 else 0
                    logger.info(
                        f"Progress: {idx:,}/{total:,} ({pct:.1f}%)  "
                        f"Updated={updated:,}  Skipped={skipped:,}  Errors={errors:,}  "
                        f"[{elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining, {rate:.0f} files/s]"
                    )

    elapsed_total = time.time() - start_time
    logger.info("=" * 60)
    logger.info(
        f"Done!  Updated={updated:,}  Skipped={skipped:,}  Errors={errors:,}  "
        f"Total time: {elapsed_total:.1f}s"
    )
    logger.info(f"Log saved to: {LOG_FILE}")


if __name__ == "__main__":
    main()
