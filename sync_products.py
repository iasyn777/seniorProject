import pyodbc
import pymysql
import uuid
from db_config import MSSQL_CONN, MYSQL_CONN

# HEX строка GUID типа цены (без кавычек, без 0x)
PRICE_TYPE_HEX = "9A6E000C2919E56D11F00E4E2FB3B2E3"

def decode_guid_strict(b: bytes) -> str:
    if len(b) != 16:
        raise ValueError("Ожидано 16 байт")

    return (
        f"{b[12]:02x}{b[13]:02x}{b[14]:02x}{b[15]:02x}-"  # 20 dd c7 07
        f"{b[10]:02x}{b[11]:02x}-"  # 13 02
        f"{b[8]:02x}{b[9]:02x}-"  # 11 f0
        f"{b[0]:02x}{b[1]:02x}-"  # 9a 6e
        f"{b[2]:02x}{b[3]:02x}{b[4]:02x}{b[5]:02x}{b[6]:02x}{b[7]:02x}"  # 00 0c 29 19 e5 6d
    )

def sync_products():
    mssql = pyodbc.connect(
        f"DRIVER={MSSQL_CONN['driver']};"
        f"SERVER={MSSQL_CONN['server']};"
        f"DATABASE={MSSQL_CONN['database']};"
        f"Trusted_Connection={MSSQL_CONN['trusted_connection']}"
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
        product_name = row[1] if row[1] is not None else None
        product_desc = row[2]
        parent_bin = row[3]
        price = row[4] if row[4] is not None else 0.0

        # Раскодировать category_id (если не пустой 0x00..00)
        category_id = None
        if parent_bin and parent_bin != b'\x00' * 16:
            category_id = decode_guid_strict(parent_bin)

        my_cursor.execute("""
                    SELECT name, description, price
                    FROM products_test
                    WHERE id = %s
                """, (product_id,))
        existing = my_cursor.fetchone()

        print(f"MSSQL: name={product_name}, desc={product_desc}, price={price}")
        print(f"MySQL: {existing}")

        # Проверка на дубли
        if existing is None:
            my_cursor.execute("""
                    INSERT INTO products_test (id, name, description, price)
                    VALUES (%s, %s, %s, %s)
                """, (product_id, product_name, product_desc, price))
            inserted += 1
        else:
            existing_id, existing_desc, existing_price = existing
            if (
                    existing_desc != product_desc or
                    round(float(existing_price), 2) != round(price, 2)
            ):
                print(f"Updating {product_name}: price {existing_price} -> {price}")
                my_cursor.execute("""
                        UPDATE products_test
                        SET id = %s, description = %s, price = %s
                        WHERE name = %s
                    """, (product_id, product_desc, price, product_name))
                updated += 1

    mysql.commit()
    print(f"✅ Добавлено: {inserted}, Обновлено: {updated}")

    ms_cursor.close()
    my_cursor.close()
    mssql.close()
    mysql.close()


if __name__ == "__main__":
    sync_products()
