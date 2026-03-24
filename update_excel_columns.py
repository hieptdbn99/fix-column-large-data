import os
import sys
import logging
import time
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import openpyxl

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BASE_FOLDER = r"E:\mesv3"
LOG_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "update_excel_order.log")

# ─────────────────────────────────────────────
# WORKER FUNCTION
# ─────────────────────────────────────────────

def process_xlsx_file(file_path_str: str) -> tuple[bool, str | None]:
    """
    Read an XLSX, reorder 'Result' before 'Remark', overwrite the file.
    Returns (changed: bool, error_or_warning: str | None).
    """
    file_path = Path(file_path_str)
    try:
        wb = openpyxl.load_workbook(file_path)
        sheet = wb.active
        
        # Get headers from the first row
        headers = [cell.value for cell in sheet[1]]
        
        # Identify indices of 'Result' and 'Remark'
        # Matching is case-insensitive for robustness
        idx_result = -1
        idx_remark = -1
        
        for i, h in enumerate(headers):
            if h is None: continue
            h_str = str(h).strip().lower()
            if h_str == "result":
                idx_result = i
            elif h_str == "remark":
                idx_remark = i
        
        # If both are found and Remark is BEFORE Result, we need to swap or reorder.
        # User wants Result before Remark.
        if idx_result == -1 or idx_remark == -1:
            return False, f"SKIP: 'Result' or 'Remark' not found in {file_path.name}"
        
        if idx_result == idx_remark - 1:
            # Already immediately before Remark
            return False, None
        
        # If we reach here, Result is NOT immediately before Remark. We need to move it.
        # Simple strategy: Move the 'Result' column to the left of 'Remark'.
        
        all_data = []
        for row in sheet.iter_rows(min_row=1, values_only=True):
            row_list = list(row)
            # Reorder row_list
            v_res = row_list.pop(idx_result)
            
            # If Result was at a higher index than Remark, it's easy:
            #   ["ID", "Remark", "Data", "Result"] (idx_rem=1, idx_res=3)
            #   pop(3) -> ["ID", "Remark", "Data"], insert(1) -> ["ID", "Result", "Remark", "Data"]
            # If Result was at a lower index than Remark, but not immediately:
            #   ["ID", "Result", "Data", "Remark"] (idx_res=1, idx_rem=3)
            #   pop(1) -> ["ID", "Data", "Remark"], insert(3-1) -> ["ID", "Data", "Result", "Remark"]
            
            new_insert_idx = idx_remark if idx_result > idx_remark else idx_remark - 1
            row_list.insert(new_insert_idx, v_res)
            all_data.append(row_list)
        
        # Clear sheet and write new data
        sheet.delete_rows(1, sheet.max_row)
        for r_idx, row_data in enumerate(all_data, 1):
            for c_idx, value in enumerate(row_data, 1):
                sheet.cell(row=r_idx, column=c_idx, value=value)
        
        wb.save(file_path)
        return True, None

    except Exception as exc:
        return False, f"ERROR {file_path.name}: {exc}"

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    # Setup logging
    logger = logging.getLogger("excel_main")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("=" * 60)
    logger.info("Excel Column Order Update (Result before Remark)")
    logger.info(f"Base folder : {BASE_FOLDER}")
    logger.info("=" * 60)

    if not os.path.isdir(BASE_FOLDER):
        logger.error(f"Folder not found: {BASE_FOLDER}")
        return

    logger.info("Scanning for YYYY-MM-DD.xlsx files...")
    # Regex to match 2024-01-01.xlsx etc.
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}\.xlsx$")
    
    xlsx_files = []
    for root, dirs, files in os.walk(BASE_FOLDER):
        for f in files:
            if date_pattern.match(f):
                xlsx_files.append(os.path.join(root, f))
    
    total = len(xlsx_files)
    if total == 0:
        logger.warning("No matching YYYY-MM-DD.xlsx files found.")
        return

    logger.info(f"Found {total:,} Excel file(s).")

    num_workers = min(8, max(2, multiprocessing.cpu_count()))
    logger.info(f"Starting processing with {num_workers} workers...")

    updated = 0
    skipped = 0
    errors  = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_path = {executor.submit(process_xlsx_file, f): f for f in xlsx_files}

        for i, future in enumerate(as_completed(future_to_path), 1):
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

            if i % 10 == 0 or i == total:
                elapsed = time.time() - start_time
                logger.info(f"Progress: {i}/{total} | Updated={updated} | Skipped={skipped} | Errors={errors} | Time={elapsed:.1f}s")

    logger.info("=" * 60)
    logger.info(f"Finished. Total Updated: {updated}, Skipped: {skipped}, Errors: {errors}")
    logger.info(f"Total time: {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()
