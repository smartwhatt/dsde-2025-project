#!/usr/bin/env python3
"""Create a small test CSV dataset from the first N rows of each file."""

import csv
import pathlib
import sys


def create_test_csvs(source_dir: str, dest_dir: str, num_rows: int = 100):
    """Copy first N rows from each CSV file to test directory."""
    source_path = pathlib.Path(source_dir)
    dest_path = pathlib.Path(dest_dir)

    if not source_path.exists():
        print(f"Error: Source directory {source_dir} does not exist")
        sys.exit(1)

    # Create destination directory
    dest_path.mkdir(exist_ok=True)
    print(f"Creating test CSV files in {dest_dir} with {num_rows} rows each...\n")

    csv_files = sorted(source_path.glob("*.csv"))

    for csv_file in csv_files:
        print(f"Processing {csv_file.name}...")

        with open(csv_file, "r", encoding="utf-8") as f_in:
            reader = csv.reader(f_in)
            header = next(reader)

            # Read up to num_rows
            rows = []
            for i, row in enumerate(reader):
                if i >= num_rows:
                    break
                rows.append(row)

        # Write to destination
        dest_file = dest_path / csv_file.name
        with open(dest_file, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(rows)

        print(f"  ✓ Wrote {len(rows)} rows to {csv_file.name}")

    print(f"\n✅ Created {len(csv_files)} test CSV files in {dest_dir}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create test CSV subset")
    parser.add_argument(
        "--source",
        default="./csv_output",
        help="Source CSV directory (default: ./csv_output)",
    )
    parser.add_argument(
        "--dest",
        default="./csv_test",
        help="Destination directory for test CSVs (default: ./csv_test)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="Number of rows to copy from each file (default: 100)",
    )

    args = parser.parse_args()
    create_test_csvs(args.source, args.dest, args.rows)
