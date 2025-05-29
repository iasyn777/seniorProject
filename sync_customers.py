import pymysql
import pyodbc
import uuid
from db_config import MSSQL_CONN, MYSQL_CONN
from utils import decode_guid_strict


# Подключение к MySQL
mysql = pymysql.connect(**MYSQL_CONN)
mssql = pyodbc.connect(**MSSQL_CONN)


try:
    with mysql.cursor() as my_cursor, mssql.cursor() as ms_cursor:
        my_cursor.execute("SELECT id, email FROM users WHERE ic_guid IS NULL")
        users = my_cursor.fetchall()

        for user_id, email in users:
            ms_cursor.execute("SELECT _IDRRef FROM _Reference222 WHERE _Fld4380 = ?", (email,))
            result = ms_cursor.fetchone()

            if result:
                guid_binary = result[0]
                guid = decode_guid_strict(guid_binary)
                my_cursor.execute("UPDATE users SET ic_guid = %s WHERE id = %s", (guid, user_id))

    mysql.commit()
    print("IC_GUID обновлены.")
finally:
    mysql.close()
    mssql.close()
