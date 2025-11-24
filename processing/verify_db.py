#!/usr/bin/env python3
"""Verify data was loaded correctly into database."""

import psycopg2
import dotenv

dotenv.load_dotenv(".env")
conn_string = dotenv.get_key(".env", "CONN_STRING")

conn = psycopg2.connect(conn_string)
cur = conn.cursor()

print("Verifying database contents:\n")

tables = [
    "sources",
    "affiliations",
    "authors",
    "subject_areas",
    "keywords",
    "funding_agencies",
    "papers",
    "paper_authors",
    "paper_author_affiliations",
    "paper_keywords",
    "paper_subject_areas",
    "reference_papers",
    "paper_funding",
]

for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    print(f"  {table:30s} {count:6,d} rows")

print("\nChecking sample data:")
cur.execute("SELECT paper_id, title, scopus_id FROM papers LIMIT 3")
print("\nPapers:")
for row in cur.fetchall():
    print(f"  ID={row[0]}, scopus={row[2]}, title={row[1][:50]}...")

cur.execute(
    "SELECT pa.paper_author_id, p.scopus_id, a.indexed_name FROM paper_authors pa JOIN papers p ON pa.paper_id = p.paper_id JOIN authors a ON pa.author_id = a.author_id LIMIT 3"
)
print("\nPaper Authors (with joins):")
for row in cur.fetchall():
    print(f"  PA_ID={row[0]}, paper={row[1]}, author={row[2]}")

cur.close()
conn.close()

print("\n✅ Database verification complete!")
