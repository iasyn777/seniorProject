from db_config import MSSQL_CONN, MYSQL_CONN
import pyodbc
import pymysql


def test_mssql():
    try:
        conn = pyodbc.connect(
            f"DRIVER={MSSQL_CONN['driver']};"
            f"SERVER={MSSQL_CONN['server']};"
            f"DATABASE={MSSQL_CONN['database']};"
            f"UID={MSSQL_CONN['uid']};"
            f"PWD={MSSQL_CONN['pwd']};"
        )
        print("✅ MSSQL (1С) connected")
        conn.close()
    except Exception as e:
        print("❌ MSSQL connection error:", e)
        print(MSSQL_CONN)


def test_mysql():
    try:
        conn = pymysql.connect(**MYSQL_CONN)
        print("✅ MySQL (сайт) connected")
        conn.close()
    except Exception as e:
        print("❌ MySQL connection error:", e)


if __name__ == "__main__":
    test_mssql()
    test_mysql()
