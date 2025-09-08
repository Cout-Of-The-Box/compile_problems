#!/usr/bin/env python3

import os
import glob
import csv
import sys
import shutil

import pandas as pd

def create_initial_csvs(dir_name, file_names, header):
    os.makedirs(dir_name, exist_ok=True)
    for name in file_names:
        path = os.path.join(dir_name, name)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(header)
    print(f"Created directory '{dir_name}' with {len(file_names)} CSV files.")

def merge_csv_files(pattern, output):
    files = sorted(glob.glob(f"**/{pattern}", recursive=True))
    print(f"\nMerging {len(files)} file(s) matching '{pattern}' → '{output}'")
    if not files:
        print(f"  [WARN] No files found for pattern '{pattern}'.")
        # Create an empty file with no rows (header-only if needed)
        open(output, "w").close()
        return

    writer = None
    total_rows = 0

    with open(output, "w", newline="", encoding="utf-8") as fout:
        for path in files:
            try:
                with open(path, newline="", encoding="utf-8") as fin:
                    reader = csv.reader(fin)
                    header = next(reader, None)
                    if header is None:
                        print(f"  [SKIP] Empty file: {path}")
                        continue

                    if writer is None:
                        writer = csv.writer(fout)
                        writer.writerow(header)
                    for row in reader:
                        writer.writerow(row)
                        total_rows += 1
                print(f"  [OK] Appended {os.path.basename(path)}")
            except Exception as e:
                print(f"  [ERROR] '{path}': {e}")

    print(f"  → Merged {len(files)} files, {total_rows} total rows.")

def merge_duplicates(input_csv, output_csv):
    title_map = {}
    total_rows = 0

    with open(input_csv, newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames
        if not fieldnames or "Title" not in fieldnames:
            print(f"  [ERROR] 'Title' column missing in {input_csv}")
            sys.exit(1)

        for row in reader:
            total_rows += 1
            title = row["Title"]
            if title in title_map:
                title_map[title]["count"] += 1
            else:
                entry = {col: row[col] for col in fieldnames}
                entry["count"] = 1
                title_map[title] = entry

    print(f"  → Processed {total_rows} rows; {len(title_map)} unique titles.")

    out_fields = fieldnames + ["count"]
    with open(output_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=out_fields)
        writer.writeheader()
        for entry in title_map.values():
            writer.writerow(entry)

    print(f"  → Wrote deduplicated CSV to '{output_csv}'.")

def sort_by_count(input_csv):
    df = pd.read_csv(input_csv)
    df_sorted = df.sort_values("count", ascending=False)
    df_sorted.to_csv(input_csv, index=False)
    print(f"  → Sorted '{input_csv}' by 'count' descending.")

def process_all():
    # 1) Setup
    dir_name = "AAA"
    file_names = [
        "1. Thirty Days.csv",
        "2. Three Months.csv",
        "3. Six Months.csv",
        "4. More Than Six Months.csv",
        "5. All.csv",
    ]
    header = "Difficulty,Title,Frequency,Acceptance Rate,Link,Topics\n"

    print("\n[STEP 1] Creating initial CSVs")
    create_initial_csvs(dir_name, file_names, header)

    # Prepare master_folder (one level up)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    master_folder = os.path.abspath(os.path.join(script_dir, os.pardir, "master_folder"))
    os.makedirs(master_folder, exist_ok=True)
    print(f"\n[STEP 2] Master folder: {master_folder}")

    # 2) Loop over each CSV type
    for pattern in file_names:
        print(f"\n[PROCESS] {pattern}")

        temp_out = "temp_output.csv"
        final_out = "final_output.csv"

        merge_csv_files(pattern, temp_out)
        merge_duplicates(temp_out, final_out)
        sort_by_count(final_out)

        base, _ = os.path.splitext(pattern)
        safe_name = f"{base}_final.csv"
        dest = os.path.join(master_folder, safe_name)
        shutil.copy(final_out, dest)

        print(f"  → Copied final result → {dest}")

    print("\n🎉 All done. Final CSVs are in:")
    print(f"    {master_folder}\n")

if __name__ == "__main__":
    process_all()
