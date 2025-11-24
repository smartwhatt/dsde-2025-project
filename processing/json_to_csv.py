"""Export Scopus JSON data to CSV files.

This script loads JSON files from a directory and exports them to normalized
CSV files representing database tables.
"""

import argparse
import json
import pathlib
import tqdm
from typing import List

from csv_exporter import ScopusCSVExporter


def load_json_files(data_dir: pathlib.Path, batch_size: int = 100) -> List[List[dict]]:
    """Load JSON files from directory and batch them.

    Args:
        data_dir: Directory containing JSON files
        batch_size: Number of files per batch

    Returns:
        List of batches, where each batch is a list of JSON objects
    """
    # Get all descendant files in the data directory
    data_files = [
        f for f in data_dir.rglob("*") if f.is_file() and not f.name.startswith(".")
    ]

    print(f"Found {len(data_files)} files to process")

    # Load and batch files
    batches = []
    current_batch = []

    for file_path in tqdm.tqdm(data_files, desc="Loading JSON files"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Handle both direct objects and wrapped responses
                if "abstracts-retrieval-response" in data:
                    json_obj = data["abstracts-retrieval-response"]
                else:
                    json_obj = data
                current_batch.append(json_obj)

                if len(current_batch) >= batch_size:
                    batches.append(current_batch)
                    current_batch = []
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            continue

    # Add remaining files as final batch
    if current_batch:
        batches.append(current_batch)

    print(f"Created {len(batches)} batches")
    return batches


def export_to_csv(
    data_dir: pathlib.Path, output_dir: pathlib.Path, batch_size: int = 100
):
    """Export JSON files to CSV format.

    Args:
        data_dir: Directory containing JSON files
        output_dir: Directory where CSV files will be written
        batch_size: Number of papers to process per batch
    """
    print(f"Exporting data from {data_dir} to CSV in {output_dir}")

    # Load JSON files in batches
    batches = load_json_files(data_dir, batch_size)

    # Create CSV exporter
    with ScopusCSVExporter(output_dir=str(output_dir)) as exporter:
        total_papers = sum(len(batch) for batch in batches)
        processed = 0

        for batch_idx, batch in enumerate(batches, 1):
            print(
                f"\nProcessing batch {batch_idx}/{len(batches)} ({len(batch)} papers)"
            )

            # Export batch
            paper_ids = exporter.export_papers_batch(
                batch,
                progress_callback=lambda current, total: None,  # Progress handled by tqdm
                task_callback=lambda task, current, total: print(f"  {task}"),
            )

            processed += len(paper_ids)
            print(f"Processed {processed}/{total_papers} papers total")

    print(f"\n✓ Export complete! CSV files saved to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Export Scopus JSON files to CSV format"
    )
    parser.add_argument(
        "--data-dir",
        type=pathlib.Path,
        default=pathlib.Path("./processing/data"),
        help="Directory containing JSON files (default: ./processing/data)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of papers per batch (default: 100)",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=pathlib.Path("./csv_output"),
        help="Output directory for CSV files (default: ./csv_output)",
    )

    args = parser.parse_args()

    # Validate data directory exists
    if not args.data_dir.exists():
        print(f"Error: Data directory {args.data_dir} does not exist")
        return

    export_to_csv(args.data_dir, args.output_dir, args.batch_size)


if __name__ == "__main__":
    main()
