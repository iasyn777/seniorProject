import pyodbc
import pymysql
import uuid
from utils import decode_guid_strict
from db_config import MSSQL_CONN, MYSQL_CONN

# HEX строка GUID типа цены (без кавычек, без 0x)
PRICE_TYPE_HEX = "9A6E000C2919E56D11F00E4E2FB3B2E3"

def sync_products():
    mssql = pyodbc.connect(
        f"DRIVER={MSSQL_CONN['driver']};"
        f"SERVER={MSSQL_CONN['server']};"
        f"DATABASE={MSSQL_CONN['database']};"
            f"UID={MSSQL_CONN['uid']};"
            f"PWD={MSSQL_CONN['pwd']};"
    )
    ms_cursor = mssql.cursor()

    mysql = pymysql.connect(**MYSQL_CONN)
    my_cursor = mysql.cursor()

    sql = """
        SELECT 
            n._IDRRef AS id_bin,
            n._Description AS name,
            n._Fld5844 AS description,
            n._ParentIDRRef AS parent_bin,
            p._Fld30657 AS price
        FROM dbo._Reference266 n
        JOIN (
        SELECT p1.*
        FROM dbo._InfoRg30653 p1
        JOIN (
            SELECT _Fld30655RRef AS NomenclatureID, MAX(_Period) AS MaxDate
            FROM dbo._InfoRg30653
            WHERE _Fld30654RRef = ?
            GROUP BY _Fld30655RRef
        ) latest
        ON p1._Fld30655RRef = latest.NomenclatureID AND p1._Period = latest.MaxDate
        WHERE p1._Fld30654RRef = ?
    ) p
    ON p._Fld30655RRef = n._IDRRef
    WHERE n._Description IS NOT NULL
    """

    price_type_binary = pyodbc.Binary(bytes.fromhex(PRICE_TYPE_HEX))
    ms_cursor.execute(sql, price_type_binary, price_type_binary)

    inserted = 0
    updated = 0

    for row in ms_cursor.fetchall():
        product_id = decode_guid_strict(row[0])  # id_bin
        product_ic_guid = row[0].hex().upper()  # HEX строка без 0x
        product_name = row[1] if row[1] is not None else None
        product_desc = row[2]
        parent_bin = row[3]
        price = row[4] if row[4] is not None else 0.0

        # Раскодировать category_id (если не пустой 0x00..00)
        category_id = None
        if parent_bin and parent_bin != b'\x00' * 16:
            category_id = decode_guid_strict(parent_bin)

            # Проверка на существование по id
            my_cursor.execute("""
                SELECT name, description, price, ic_guid
                FROM products_test
                WHERE id = %s
            """, (product_id,))
            existing = my_cursor.fetchone()

            if existing is None:
                my_cursor.execute("""
                    INSERT INTO products_test (id, ic_guid, name, description, price)
                    VALUES (%s, %s, %s, %s, %s)
                """, (product_id, product_ic_guid, product_name, product_desc, price))
                inserted += 1
            else:
                existing_name, existing_desc, existing_price, existing_guid = existing
                if (
                        existing_desc != product_desc or
                        round(float(existing_price), 2) != round(price, 2)
                ):
                    my_cursor.execute("""
                        UPDATE products_test
                        SET name = %s, description = %s, price = %s, ic_guid = %s
                        WHERE id = %s
                    """, (product_name, product_desc, price, product_ic_guid, product_id))
                    updated += 1

            # Доп реквизиты

        ms_cursor.execute("""
               SELECT VT._Fld5925RRef, VT._Fld5926_RRRef
               FROM dbo._Reference266_VT5923 VT
               WHERE VT._Reference266_IDRRef = ?
           """, row[0])

        for rec in ms_cursor.fetchall():
            prop_id_bin, val_id_bin = rec
            prop_id = decode_guid_strict(prop_id_bin)
            val_id = decode_guid_strict(val_id_bin)

            # Название реквизита
            ms_cursor.execute("SELECT _Description FROM dbo._Chrc1508 WHERE _IDRRef = ?", prop_id_bin)
            prop_name_row = ms_cursor.fetchone()
            if not prop_name_row: continue
            prop_name = prop_name_row[0]

            # Значение реквизита
            ms_cursor.execute("SELECT _Description FROM dbo._Reference173 WHERE _IDRRef = ?", val_id_bin)
            val_name_row = ms_cursor.fetchone()
            if not val_name_row: continue
            val_name = val_name_row[0]

            # INSERT property
            my_cursor.execute("""
                        INSERT INTO properties_test (id, name)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE name=VALUES(name)
                    """, (prop_id, prop_name))

            # INSERT property_value
            my_cursor.execute("""
                INSERT INTO property_values_test (id, property_id, name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE name=VALUES(name), property_id=VALUES(property_id)
            """, (val_id, prop_id, val_name))

            # INSERT product_property
            my_cursor.execute("""
                        INSERT INTO product_properties_test (product_id, property_id, property_value_id	)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE property_value_id	=VALUES(property_value_id)
                    """, (product_id, prop_id, val_id))

    mysql.commit()
    print(f"✅ Добавлено: {inserted}, Обновлено: {updated}")

    ms_cursor.close()
    my_cursor.close()
    mssql.close()
    mysql.close()


if __name__ == "__main__":
    sync_products()
