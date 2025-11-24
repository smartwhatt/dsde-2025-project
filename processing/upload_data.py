import asyncio
import json
import pathlib
import dotenv
import tqdm

from handler import AsyncScopusDBLoader

dotenv.load_dotenv(".env")

# Get all descendant files in the 'data' directory
data_directory = pathlib.Path("./processing/data")

data_files = [
    f for f in data_directory.rglob("*") if f.is_file() and not f.name.startswith(".")
]

conn_string = dotenv.get_key(".env", "CONN_STRING")


async def _process_file(semaphore, async_loader, file_path):
    async with semaphore:
        # Read file synchronously (fast) and then run DB work in threadpool via the async loader
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)["abstracts-retrieval-response"]
        pid = await async_loader.insert_paper_async(data)
        return file_path, pid


async def main(concurrency: int = 8):
    semaphore = asyncio.Semaphore(concurrency)
    async_loader = AsyncScopusDBLoader(conn_string, max_workers=concurrency)

    tasks = [
        asyncio.create_task(_process_file(semaphore, async_loader, p))
        for p in data_files
    ]

    results = []
    try:
        for fut in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            file_path, pid = await fut
            results.append((file_path, pid))
    finally:
        await async_loader.close()

    # Optionally inspect results
    for path, pid in results:
        print(f"Processed {path} -> {pid}")


if __name__ == "__main__":
    import os

    # concurrency can be set via env var or CLI in future
    concurrency = int(os.getenv("UPLOAD_CONCURRENCY", "8"))
    asyncio.run(main(concurrency))
