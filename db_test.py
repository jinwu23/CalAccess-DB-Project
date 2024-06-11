import os
from dotenv import load_dotenv

from db_connect import db_connect

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# print all tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()
print("Table Count: ", cursor.rowcount)
for table in tables:
    print(table[0])
