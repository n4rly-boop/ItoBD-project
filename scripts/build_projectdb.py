#!/usr/bin/env python3
from pathlib import Path
import sys

import psycopg2


ROOT_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT_DIR / "sql"
DATA_DIR = ROOT_DIR / "data"
SECRETS_DIR = ROOT_DIR / "secrets"

DB_CONFIG = {
    "host": "hadoop-04.uni.innopolis.ru",
    "port": 5432,
    "user": "team22",
    "dbname": "team22_projectdb",
}


def read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_password(path: Path) -> str:
    return read_text_file(path).strip()


def split_sql_queries(sql_text: str):
    queries = []
    for part in sql_text.split(";"):
        query = part.strip()
        if query:
            queries.append(query + ";")
    return queries


def main() -> int:
    create_tables_sql = read_text_file(SQL_DIR / "create_tables.sql")
    import_data_sql = read_text_file(SQL_DIR / "import_data.sql")
    test_database_sql = read_text_file(SQL_DIR / "test_database.sql")
    password = read_password(SECRETS_DIR / ".psql.pass")
    csv_path = DATA_DIR / "flight_data_2024.csv"

    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
        return 1

    conn = None
    cur = None

    try:
        conn = psycopg2.connect(password=password, **DB_CONFIG)
        cur = conn.cursor()

        print("Creating tables...")
        cur.execute(create_tables_sql)
        conn.commit()
        print("Tables created.")

        print("Importing CSV data...")
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            cur.copy_expert(import_data_sql, f)
        conn.commit()
        print("Data imported.")

        print("Running test queries...")
        for idx, query in enumerate(split_sql_queries(test_database_sql), start=1):
            print(f"\n--- Query {idx} ---")
            print(query.strip())
            cur.execute(query)

            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                print("Columns:", columns)
                for row in rows:
                    print(row)

        conn.commit()
        print("\nDatabase build completed successfully.")
        return 0

    except Exception as e:
        if conn is not None:
            conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
