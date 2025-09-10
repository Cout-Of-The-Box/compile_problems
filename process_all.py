#!/usr/bin/env python3

import os
import glob
import csv
import sys
import shutil
import argparse
import pandas as pd

# ─── GLOBAL HEADER ─────────────────────────────────────────────────────────────
HEADER_LIST = [
    "Difficulty",
    "Title",
    "Frequency",
    "Acceptance Rate",
    "Link",
    "Topics",
]
HEADER_LINE = ",".join(HEADER_LIST) + "\n"
# ────────────────────────────────────────────────────────────────────────────────

def create_initial_csvs(dir_name, file_names, header_line):
    os.makedirs(dir_name, exist_ok=True)
    for name in file_names:
        path = os.path.join(dir_name, name)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(header_line)
    print(f"Created directory '{dir_name}' with {len(file_names)} CSV files.")

def load_companies_from_file(path):
    """Read one folder name per line from a file."""
    companies = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if row and row[0].strip():
                companies.add(row[0].strip())
    return companies

def parse_companies_arg(arg):
    """
    If `arg` is a file path, load lines from it.
    Otherwise split by comma.
    """
    if not arg:
        return None
    arg = arg.strip()
    if os.path.isfile(arg):
        print(f"Loading companies from file: {arg}")
        return load_companies_from_file(arg)
    parts = [x.strip() for x in arg.split(",") if x.strip()]
    print(f"Using inline companies list: {parts}")
    return set(parts)

def merge_csv_files(pattern, output, allowed_dirs=None):
    """
    Merge all CSVs matching `pattern` into `output`.
    Only folders in `allowed_dirs` are scanned if provided.
    Always writes HEADER_LIST first, then each row by dict lookup.
    """
    # 1) gather matching paths
    if allowed_dirs:
        files = []
        for d in allowed_dirs:
            files += glob.glob(os.path.join(d, "**", pattern), recursive=True)
    else:
        files = glob.glob(os.path.join("**", pattern), recursive=True)
    files = sorted(files)

    print(f"\nMerging {len(files)} file(s) matching '{pattern}' → '{output}'")

    # 2) open output and write full header
    with open(output, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(HEADER_LIST)

        if not files:
            print(f"  [WARN] No files found; output will only have header.")
            return

        row_count = 0
        # 3) read each file as DictReader, output only HEADER_LIST columns
        for path in files:
            try:
                with open(path, newline="", encoding="utf-8") as fin:
                    reader = csv.DictReader(fin)
                    for row in reader:
                        out = [row.get(col, "") for col in HEADER_LIST]
                        writer.writerow(out)
                        row_count += 1
                print(f"  [OK] Appended {os.path.basename(path)}")
            except Exception as e:
                print(f"  [ERROR] '{path}': {e}")

    print(f"  → Merged {len(files)} files, {row_count} total rows.")

def merge_duplicates(input_csv, output_csv):
    title_map = {}
    total_rows = 0

    with open(input_csv, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fields = reader.fieldnames or HEADER_LIST
        if "Title" not in fields:
            print(f"  [ERROR] 'Title' column missing in {input_csv}")
            sys.exit(1)

        for row in reader:
            total_rows += 1
            t = row["Title"]
            if t in title_map:
                title_map[t]["count"] += 1
            else:
                entry = {col: row.get(col, "") for col in fields}
                entry["count"] = 1
                title_map[t] = entry

    print(f"  → Processed {total_rows} rows; {len(title_map)} unique titles.")

    out_fields = fields + ["count"]
    with open(output_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(title_map.values())
    print(f"  → Wrote deduplicated CSV to '{output_csv}'.")

def sort_by_count(input_csv):
    df = pd.read_csv(input_csv)
    df.sort_values("count", ascending=False, inplace=True)
    df.to_csv(input_csv, index=False)
    print(f"  → Sorted '{input_csv}' by 'count' descending.")

def process_all(companies_arg):
    # Work in script's folder
    base = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base)

    # STEP 1: create header-only CSVs
    dir_name = "AAA"
    file_names = [
      "1. Thirty Days.csv",
      "2. Three Months.csv",
      "3. Six Months.csv",
      "4. More Than Six Months.csv",
      "5. All.csv",
    ]
    print("\n[STEP 1] Creating initial CSVs")
    create_initial_csvs(dir_name, file_names, HEADER_LINE)

    # STEP 2: prepare master_folder
    master = os.path.join(base, "master_folder")
    os.makedirs(master, exist_ok=True)
    print(f"\n[STEP 2] Master folder → {master}")

    # STEP 3: parse filter
    allowed = parse_companies_arg(companies_arg)
    if allowed:
        print(f"\n[STEP 3] Including only folders: {sorted(allowed)}")

    # STEP 4: process each pattern
    for pattern in file_names:
        print(f"\n[PROCESS] {pattern}")
        tmp_csv = "temp_output.csv"
        final_csv = "final_output.csv"

        merge_csv_files(pattern, tmp_csv, allowed)
        merge_duplicates(tmp_csv, final_csv)
        sort_by_count(final_csv)

        base_name, _ = os.path.splitext(pattern)
        out_name = f"{base_name}_final.csv"
        dest = os.path.join(master, out_name)
        shutil.copy(final_csv, dest)
        print(f"  → Copied {dest}")

    print(f"\n🎉 Done! All final CSVs (with Topics) are in:\n    {master}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
      description="Merge → dedupe → sort CSVs; optional folder‐level filter."
    )
    parser.add_argument(
      "-c","--companies",
      help="Comma‐list of folders (A,B,…) or path to file (one per line)."
    )
    args = parser.parse_args()
    process_all(args.companies)
