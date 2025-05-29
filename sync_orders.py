import pymysql, pyodbc, uuid
from db_config import MYSQL_CONN, MSSQL_CONN
from utils import decode_guid_strict  # ваша функция 0x.. → UUID


def build_conn(d):  # для pyodbc
    return ';'.join(f'{k}={v}' for k, v in d.items())


mysql = pymysql.connect(**MYSQL_CONN)
mssql = pyodbc.connect(build_conn(MSSQL_CONN))

try:
    with mysql.cursor() as my_cur, mssql.cursor() as ms_cur:
        # 1. берём статусы из _Reference400 и делаем dict GUID → Название
        ms_cur.execute("SELECT _IDRRef, _Description FROM _Reference400")
        status_map = {decode_guid_strict(row[0]): row[1] for row in ms_cur}

        # 2. вытаскиваем заказы из _Document559
        ms_cur.execute("""
            SELECT _IDRRef, _Number, _Date_Time, _Posted, _Fld12709_RRRef, _Fld12723
            FROM _Document559
        """)
        rows = ms_cur.fetchall()

        insert_sql = """INSERT IGNORE INTO orders_history
                        (doc_number, doc_date, posted, status_id, status_name,
                         total_sum, email)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)"""

        for r in rows:
            raw_guid = r[0]  # GUID документа (можно хранить при желании)
            number = r[1].strip()
            dt = r[2]
            posted = 1 if r[3] == b'\x01' else 0
            status_guid = decode_guid_strict(r[4])
            status_name = status_map.get(status_guid, 'Неизв.')
            total = float(r[5])

            # e-mail берём из полного наименования контрагента:
            #   сначала найдём контрагента-владельца
            ms_cur.execute("""
                SELECT C._Fld4380
                FROM _Document559 D
                JOIN _Reference222 C ON C._IDRRef = D._Fld12675RRef   -- поле ссылки на контрагента
                WHERE D._IDRRef = ?
            """, (raw_guid,))
            email_row = ms_cur.fetchone()
            email = email_row[0].strip() if email_row else ''

            my_cur.execute(insert_sql,
                           (number, dt, posted, status_guid,
                            status_name, total, email)
                           )

        mysql.commit()
        print('Синхронизация заказов завершена')
finally:
    mysql.close()
    mssql.close()
