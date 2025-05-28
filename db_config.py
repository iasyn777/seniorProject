import os
from dotenv import load_dotenv

load_dotenv()

MSSQL_CONN = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': '213.109.67.27, 1433',
    'database': 'diplom',
    'uid': 'user',
    'pwd': 'E6DzQo#Q47CeE6DzQo#Q47Ce',
}


MYSQL_CONN = {
    'host': '127.0.0.1',
    'port': 3306,  # туннель идёт через этот порт
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASS'),
    'database': 'coffeegrainy_shop',
    'charset': 'utf8mb4'
}


