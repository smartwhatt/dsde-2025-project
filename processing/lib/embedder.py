import pandas as pd
from sqlalchemy import Engine, text
import ollama
from psycopg2.extras import execute_values
from tqdm import tqdm


MAX_CHARS = 20000
BATCH_SIZE = 64   # safe for nomic-embed-text


def truncate(text: str, max_chars=MAX_CHARS):
    if not isinstance(text, str):
        return ""
    return text[:max_chars]


class PaperEmbedder:
    def __init__(self, engine: Engine, model="nomic-embed-text"):
        self.engine = engine
        self.model = model

    # --------------------------
    # Batch embedding using ollama.embed()
    # --------------------------
    def embed_batch(self, texts):
        # ensure all text is truncated
        texts = [truncate(t) for t in texts]

        # call ollama.embed() which supports batch properly
        resp = ollama.embed(
            model=self.model,
            input=texts,
            truncate=True   # ensure long inputs don't break
        )
        return resp["embeddings"]

    # --------------------------
    # SQL fetch
    # --------------------------
    def fetch_paper_texts(self):
        query = text("""
            SELECT
                p.paper_id,
                CONCAT_WS(
                    '\n',
                    'Title: ' || p.title,
                    'Abstract: ' || COALESCE(p.abstract, ''),
                    'Keywords: ' || COALESCE(keywords.keyword_list, '')
                ) AS embed_text

            FROM papers p
            LEFT JOIN (
                SELECT 
                    pk.paper_id,
                    STRING_AGG(DISTINCT k.keyword, ', ' ORDER BY k.keyword) AS keyword_list
                FROM paper_keywords pk
                JOIN keywords k ON pk.keyword_id = k.keyword_id
                GROUP BY pk.paper_id
            ) AS keywords ON keywords.paper_id = p.paper_id;
        """)

        with self.engine.connect() as connection:
            return pd.read_sql_query(query, connection)

    # --------------------------
    # Insert into DB
    # --------------------------
    def insert_embeddings(self, df):
        rows = [
            (
                int(r["paper_id"]),
                self.model,
                "combined",
                r["embedding"]
            )
            for _, r in df.iterrows()
        ]

        sql = """
            INSERT INTO paper_embeddings (paper_id, model, source, embedding)
            VALUES %s
            ON CONFLICT (paper_id, model, source)
            DO UPDATE SET embedding = EXCLUDED.embedding;
        """

        conn = self.engine.raw_connection()
        try:
            cur = conn.cursor()
            execute_values(cur, sql, rows)
            conn.commit()
            cur.close()
        finally:
            conn.close()

        print(f"Inserted/updated {len(rows)} embeddings.")

    # --------------------------
    # Main pipeline
    # --------------------------
    def calculate_embedding(self):
        df = self.fetch_paper_texts()
        rows = df.to_dict(orient="records")

        # Sequential batching (safe)
        embeddings = []
        for i in tqdm(range(0, len(rows), BATCH_SIZE)):
            batch = rows[i:i+BATCH_SIZE]
            texts = [r["embed_text"] for r in batch]

            batch_embeddings = self.embed_batch(texts)
            embeddings.extend(batch_embeddings)

        # Attach embeddings
        df["embedding"] = embeddings

        self.insert_embeddings(df)
        return df
