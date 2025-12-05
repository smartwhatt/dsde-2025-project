# 📚 DSDE 2025 — Research Insights Platform  
A full-stack system for **bulk Scopus ingestion**, **analytics**, and **AI-powered semantic search** using **PostgreSQL + pgvector**, **Dash**, and an **Ollama-based RAG engine**.

---

# 🚀 Features

### ✔ Bulk Scopus JSON → PostgreSQL ingestion  
- Sequential batching (safe re-runs, rollback on batch failure)  
- Inserts authors, affiliations, keywords, subjects, funding, references  
- Generates embeddings (768-dim vectors) and stores them in `paper_embeddings`

### ✔ PostgreSQL + pgvector  
- Fast semantic search via `<=>` vector distance  
- Optimized schema (indexes, views, normalization)

### ✔ Dash Web Application (`app/`)
- Papers explorer  
- Author analytics  
- Affiliations explorer  
- Paper info viewer  
- **Chat-based RAG research assistant**

### ✔ RAG Engine (`app/lib/rag_engine.py`)
- Embedding generation using **Ollama → nomic-embed-text**  
- Context retrieval (pgvector)  
- Answer generation using local LLM (Qwen, Llama, etc.)  
- Returns citations and relevance scores  

---

# 🗂 Project Structure

```text
smartwhatt-dsde-2025-project/
│
├── initialize_table.sql
├── pyproject.toml
├── main.py
│
├── processing/
│   ├── json_to_csv.py
│   ├── load_csv_to_db.py
│   ├── calculate_embedding_to_db.py
│   ├── verify_db.py
│   └── lib/
│       ├── embedder.py
│       ├── csv_exporter.py
│       └── csv_to_db_loader.py
│
└── app/
    ├── main.py
    ├── database.py
    ├── lib/
    │   └── rag_engine.py
    └── pages/
        ├── home.py
        ├── chat.py
        ├── papers.py
        ├── paper_info.py
        ├── author_profile.py
        ├── affiliations.py
        ├── faculty.py
        └── test.py
```

# ⚙️ Full Setup Guide

Below is the complete setup process, including database, Ollama, and processing pipeline.

## 1️⃣ Install Required Software

```bash
# Python (3.11 recommended)
sudo apt install python3 python3-pip

# PostgreSQL
sudo apt install postgresql postgresql-contrib

# pgvector extension
sudo apt install postgresql-16-pgvector

# Ollama
curl -fsSL https://ollama.com/install.sh | sh
```

## 2️⃣ Clone the Repository

```bash
git clone https://github.com/<your-repo>/smartwhatt-dsde-2025-project.git
cd smartwhatt-dsde-2025-project
```

## 3️⃣ Create a Virtual Environment

```bash
uv venv
source .venv/bin/activate
uv sync
```

## 4️⃣ Configure .env

Create a file in project root:

```properties
CONN_STRING=postgresql://postgres:YOUR_PASSWORD@localhost:5432/dsde
```

## 5️⃣ Setup PostgreSQL Database

Create DB
```bash
sudo -u postgres psql
CREATE DATABASE dsde;
\c dsde;
```
Enable pgvector
```sql
CREATE EXTENSION vector;
```

Load schema
```bash
psql -U postgres -d dsde -f initialize_table.sql
```

## 6️⃣ Install Ollama Models

```bash
ollama pull nomic-embed-text
ollama pull qwen2.5:7b
```

## 7️⃣ Preprocessing Pipeline (Correct CLI Examples)

This project includes **three preprocessing stages**, and **each script uses argparse**, so you must run them with the correct flags.

Below are the **exact, correct commands** based on the real argparse definitions from your project files.

---

✅ Step 1 — Convert JSON → CSV  
**Script:** `processing/json_to_csv.py`  
**Source:** uses argparse with:  
- `--data-dir` (path to JSON files)  
- `--output-dir` (where CSVs will be created)  
- `--batch-size` (papers per batch)

```bash
python processing/json_to_csv.py \
    --data-dir ./processing/data \
    --output-dir ./csv_output \
    --batch-size 100
```

✅ Step 2 — Load CSVs into PostgreSQL

**Script**: `processing/load_csv_to_db.py`
**Source**: argparse in the script provides:

- --csv-dir (directory containing CSV files)

- --clear (optional flag to truncate tables first)

```bash
python processing/load_csv_to_db.py \
    --csv-dir ./csv_output \
    --clear
```

✅ Step 3 — Generate Paper Embeddings

**Script**: `processing/calculate_embedding_to_db.py`
**Source**: does not use argparse, runs immediately.

```bash
python processing/calculate_embedding_to_db.py
```

