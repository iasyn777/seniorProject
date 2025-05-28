# sync_stock.py
import pyodbc
import pymysql
from db_config import MSSQL_CONN, MYSQL_CONN
from utils import decode_guid_strict   # та самая функция декодирования GUID 1С

def sync_stock():
    # ─── MSSQL ────────────────────────────────────────────────────────────────
    mssql = pyodbc.connect(
        f"DRIVER={MSSQL_CONN['driver']};"
        f"SERVER={MSSQL_CONN['server']};"
        f"DATABASE={MSSQL_CONN['database']};"
        f"UID={MSSQL_CONN['uid']};"
        f"PWD={MSSQL_CONN['pwd']};"
    )
    ms = mssql.cursor()

    # ─── MySQL ────────────────────────────────────────────────────────────────
    mysql = pymysql.connect(**MYSQL_CONN)
    my = mysql.cursor()

    # ─── Запрос остатков (количество - резерв) по каждой номенклатуре ────────
    ms.execute("""
        SELECT
            _Fld27201RRef            AS NomenclatureID,
            SUM(COALESCE(_Fld27204,0) - COALESCE(_Fld27205,0))  AS Qty
        FROM dbo._InfoRg27198
        GROUP BY _Fld27201RRef
        HAVING SUM(COALESCE(_Fld27204,0) - COALESCE(_Fld27205,0)) <> 0
    """)

    inserted, updated = 0, 0
    for nom_bin, qty in ms.fetchall():
        product_id = decode_guid_strict(nom_bin)

        # upsert в stock_test
        my.execute("SELECT quantity FROM stock_test WHERE product_id = %s", (product_id,))
        row = my.fetchone()

        if row is None:
            my.execute(
                "INSERT INTO stock_test (product_id, quantity) VALUES (%s, %s)",
                (product_id, qty)
            )
            inserted += 1
        elif float(row[0]) != float(qty):
            my.execute(
                "UPDATE stock_test SET qty = %s WHERE product_id = %s",
                (qty, product_id)
            )
            updated += 1

    mysql.commit()
    print(f"✅ Остатки синхронизированы. Добавлено: {inserted}, Обновлено: {updated}")

    # ─── Закрыть соединения ──────────────────────────────────────────────────
    ms.close()
    mssql.close()
    my.close()
    mysql.close()

if __name__ == "__main__":
    sync_stock()
