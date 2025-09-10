#!/usr/bin/env python3

import os
import glob
import csv
import sys
import shutil
import argparse
import pandas as pd

#!/usr/bin/env python3
"""
process_all.py

DESCRIPTION
    Scans one or more company‐named subfolders for five CSV files each:
        1. "1. Thirty Days.csv"
        2. "2. Three Months.csv"
        3. "3. Six Months.csv"
        4. "4. More Than Six Months.csv"
        5. "5. All.csv"
    Merges them by pattern, deduplicates on Title (adding a `count` column),
    sorts descending by count, and writes final CSVs into an output folder.

DIRECTORY STRUCTURE
    script_dir/
    ├─ process_all.py
    ├─ CompanyA/
    │    ├─ 1. Thirty Days.csv
    │    ├─ … (all five CSVs)
    ├─ CompanyB/
    │    └─ …
    └─ …

USAGE
    # Process every subfolder under script_dir/
    python3 process_all.py

    # Process only specific folders by name
    python3 process_all.py -c "CompanyA,CompanyB"

    # Process folders listed one per line in a file
    python3 process_all.py -c companies.txt

OPTIONS
    -c, --companies   OPTIONAL filter.  
                      Either a comma-separated list of folder names:
                          "Google,Amazon,Microsoft"  
                      Or a path to a text file (one name per line).

BEHAVIOR
    * If no `-c` is provided, scans every subdirectory and writes to:
          all_companies_compiled/
    * If `-c` is provided, only those listed folders are scanned and writes to:
          custom_companies_compiled/

OUTPUT
    - One final CSV per pattern, named:
        "1. Thirty Days_final.csv", …, "5. All_final.csv"  
      placed inside the chosen output folder.  
    - Intermediate files `temp_output.csv` and `final_output.csv` are created
      in the script directory but not retained in the final output folder.

DEPENDENCIES
    - Python 3.x  
    - pandas (for sorting step)

AUTHOR
    Your Name
    Date: YYYY-MM-DD
"""


# ─── GLOBAL HEADER ─────────────────────────────────────────────────────────────
HEADER_LIST = [
    "Difficulty",
    "Title",
    "Frequency",
    "Acceptance Rate",
    "Link",
    "Topics",
]
# ────────────────────────────────────────────────────────────────────────────────

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
    # gather matching paths
    if allowed_dirs:
        files = []
        for d in allowed_dirs:
            files += glob.glob(os.path.join(d, "**", pattern), recursive=True)
    else:
        files = glob.glob(os.path.join("**", pattern), recursive=True)
    files = sorted(files)

    print(f"\nMerging {len(files)} file(s) matching '{pattern}' → '{output}'")

    with open(output, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(HEADER_LIST)

        if not files:
            print(f"  [WARN] No files found for pattern '{pattern}'. Header only.")
            return

        row_count = 0
        for path in files:
            try:
                with open(path, newline="", encoding="utf-8") as fin:
                    reader = csv.DictReader(fin)
                    for row in reader:
                        writer.writerow([row.get(col, "") for col in HEADER_LIST])
                        row_count += 1
                print(f"  [OK] Appended {os.path.basename(path)}")
            except Exception as e:
                print(f"  [ERROR] reading '{path}': {e}")

    print(f"  → Merged {len(files)} files, {row_count} rows.")

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
    # work in script directory
    base = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base)

    # patterns to process
    patterns = [
        "1. Thirty Days.csv",
        "2. Three Months.csv",
        "3. Six Months.csv",
        "4. More Than Six Months.csv",
        "5. All.csv",
    ]

    # parse company-folder filter
    allowed = parse_companies_arg(companies_arg)
    if allowed:
        print(f"Including only folders: {sorted(allowed)}")

    # choose output folder name
    out_folder = (
        "__custom_companies_compiled" if allowed
        else "__all_companies_compiled"
    )
    master = os.path.join(base, out_folder)
    os.makedirs(master, exist_ok=True)
    print(f"\nOutput folder → {master}")

    # process each CSV pattern
    for pattern in patterns:
        print(f"\n[PROCESS] {pattern}")
        tmp_csv = "temp_output.csv"
        final_csv = "final_output.csv"

        merge_csv_files(pattern, tmp_csv, allowed)
        merge_duplicates(tmp_csv, final_csv)
        sort_by_count(final_csv)

        base_name, _ = os.path.splitext(pattern)
        out_name = f"{base_name}.csv"
        dest = os.path.join(master, out_name)
        shutil.copy(final_csv, dest)
        print(f"  → Copied {out_name}")

    print(f"\n🎉 Done! All final CSVs are in:\n    {master}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge → dedupe → sort CSVs from company folders; optional filter."
    )
    parser.add_argument(
        "-c", "--companies",
        help="Comma-separated list of folder names, or path to file (one per line)."
    )
    args = parser.parse_args()
    process_all(args.companies)