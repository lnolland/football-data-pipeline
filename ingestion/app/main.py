import os
import json
from app.db import get_conn, init_db
from app.ingest import run_ingestion

def main():
    # init schema (safe pr relancer)
    init_db()

    competition = os.environ.get("COMPETITION_CODE", "FL1")  # FL1=Ligue1, PL=Premier League
    date_from = os.environ.get("DATE_FROM")  # YYYY-MM-DD optionnel
    date_to = os.environ.get("DATE_TO")      # YYYY-MM-DD optionnel

    conn = get_conn()
    try:
        conn.autocommit = False
        result = run_ingestion(conn, competition, date_from, date_to)
        conn.commit()
        print(json.dumps({"level": "info", "event": "ingestion_done", **result}, ensure_ascii=False))
    except Exception as e:
        conn.rollback()
        print(json.dumps({"level": "error", "event": "ingestion_failed", "error": str(e)}, ensure_ascii=False))
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
