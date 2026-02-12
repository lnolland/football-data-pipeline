import os
import psycopg2

def get_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

def init_db():
    from pathlib import Path
    schema_path = Path(__file__).with_name("schema.sql")
    ddl = schema_path.read_text(encoding="utf-8")

    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()
