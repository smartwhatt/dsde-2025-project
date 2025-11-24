"""Upload Scopus JSON data to PostgreSQL database.

This script loads JSON files from a directory and uploads them to a database
using the AsyncScopusDBLoader handler.
"""

import argparse
import asyncio
import json
import pathlib
import dotenv
import tqdm
from typing import List

from handler import AsyncScopusDBLoader

dotenv.load_dotenv(".env")


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


async def upload_to_database(
    data_dir: pathlib.Path,
    batch_size: int = 100,
    concurrency: int = 8,
    preupsert: bool = False,
):
    """Upload JSON files to database.

    Args:
        data_dir: Directory containing JSON files
        batch_size: Number of papers to process per batch
        concurrency: Number of concurrent workers
        preupsert: Whether to pre-upsert metadata before processing papers
    """
    print(f"Uploading data from {data_dir} to database")

    conn_string = dotenv.get_key(".env", "CONN_STRING")
    if not conn_string:
        raise ValueError("CONN_STRING not found in .env file")

    # Load JSON files in batches
    batches = load_json_files(data_dir, batch_size)

    # Create async loader
    async_loader = AsyncScopusDBLoader(
        conn_string, max_workers=concurrency, disable_metadata_upsert=not preupsert
    )

    try:
        total_papers = sum(len(batch) for batch in batches)
        processed = 0

        for batch_idx, batch in enumerate(batches, 1):
            print(
                f"\nProcessing batch {batch_idx}/{len(batches)} ({len(batch)} papers)"
            )

            # Upload batch
            paper_ids = await async_loader.insert_papers_batch_async(
                batch,
                commit=True,
                progress_callback=lambda current, total: None,  # Progress handled by tqdm
                task_callback=lambda task, current, total: print(f"  {task}"),
            )

            processed += len(paper_ids)
            print(f"Uploaded {processed}/{total_papers} papers total")

    finally:
        await async_loader.close()

    print(f"\n✓ Upload complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Upload Scopus JSON files to PostgreSQL database"
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
        "--concurrency",
        type=int,
        default=8,
        help="Number of concurrent workers (default: 8)",
    )
    parser.add_argument(
        "--preupsert",
        action="store_true",
        help="Pre-upsert metadata before processing papers",
    )

    args = parser.parse_args()

    # Validate data directory exists
    if not args.data_dir.exists():
        print(f"Error: Data directory {args.data_dir} does not exist")
        return

    asyncio.run(
        upload_to_database(
            args.data_dir, args.batch_size, args.concurrency, args.preupsert
        )
    )


if __name__ == "__main__":
    main()
