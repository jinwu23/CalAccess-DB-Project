import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Filer:
    FilerId: int

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all filers
filerIds = []
cursor.execute("""
               SELECT DISTINCT FILER_ID
               FROM FILERS_CD
               """)
db_filerIds = cursor.fetchall()
print("Unique Filers: ", cursor.rowcount)
for filerId in db_filerIds:
    filerIds.append(
        filerId[0]
    )
cursor.close()
connection.close()

# inset filerIds into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT INTO Filer (FilerId)
                VALUES (%s)
                """

data_to_insert = [
    (filerId,)
    for filerId in filerIds
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
