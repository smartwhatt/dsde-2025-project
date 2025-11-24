import json
import pathlib
import dotenv
import psycopg2

from handler import ScopusDBLoader

dotenv.load_dotenv(".env")

# Get all descendant files in the 'data' directory
data_directory = pathlib.Path("./processing/data")

data_files = [
    f for f in data_directory.rglob("*") if f.is_file() and not f.name.startswith(".")
]

conn_string = dotenv.get_key(".env", "CONN_STRING")
conn = psycopg2.connect(conn_string)
loader = ScopusDBLoader(conn=conn)


try:
    for file_path in data_files[:3]:
        if file_path.is_file():
            print(f"Processing file: {file_path}")
            with open(file_path, "r") as file:
                data = json.load(file)["abstracts-retrieval-response"]
                loader.insert_paper(data)
except Exception:
    # Caller can decide to abort the whole batch
    conn.rollback()
    raise
finally:
    conn.commit()
    loader.close()
    conn.close()
