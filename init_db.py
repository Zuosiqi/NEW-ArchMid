import argparse
from pathlib import Path

import pymysql


def connect_without_db(host: str, user: str, password: str, charset: str):
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        charset=charset,
        autocommit=False,
    )


def run_sql_file(conn, sql_path: Path):
    sql_script = sql_path.read_text(encoding="utf-8")

    # 极简分割：本项目 init_db.sql 中不包含存储过程/触发器等复杂语句，按 ; 分割足够
    statements = [s.strip() for s in sql_script.split(";") if s.strip()]

    with conn.cursor() as cursor:
        for stmt in statements:
            cursor.execute(stmt)


def main():
    parser = argparse.ArgumentParser(description="初始化 DBMSlab 数据库（执行 init_db.sql）")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="Z9J7R1R1")
    parser.add_argument("--charset", default="utf8mb4")
    parser.add_argument("--sql", default="init_db.sql", help="DDL 脚本路径（默认同目录 init_db.sql）")
    args = parser.parse_args()

    sql_path = Path(args.sql).expanduser().resolve()
    if not sql_path.exists():
        raise FileNotFoundError(f"找不到 SQL 脚本：{sql_path}")

    conn = connect_without_db(args.host, args.user, args.password, args.charset)
    try:
        run_sql_file(conn, sql_path)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("✅ 数据库初始化完成。")


if __name__ == "__main__":
    main()

