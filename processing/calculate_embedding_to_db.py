from lib.embedder import PaperEmbedder
from sqlalchemy import create_engine
import dotenv

# Load connection string
conn_string = dotenv.get_key(".env", "CONN_STRING")

# Create single engine instance


if __name__ == "__main__":
    engine = create_engine(
        conn_string,
        pool_size=10,          # Adjust based on your needs
        max_overflow=20,       # Extra connections during high load
        pool_pre_ping=True,    # Check connection health
        pool_recycle=3600,     # Recycle after 1 hour
        echo=False             # Set True for SQL debugging
    )
    embedder = PaperEmbedder(engine)
    df = embedder.calculate_embedding()

    