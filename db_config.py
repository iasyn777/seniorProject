import os
from dotenv import load_dotenv

load_dotenv()

MSSQL_CONN = {
    'driver': '{SQL Server}',
    'server': 'localhost\\SQLEXPRESS',
    'database': 'diplom',
    'trusted_connection': 'yes'
}
MYSQL_CONN = {
    'host': '127.0.0.1',
    'port': 3306,  # туннель идёт через этот порт
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASS'),
    'database': 'coffeegrainy_shop',
    'charset': 'utf8mb4'
}


