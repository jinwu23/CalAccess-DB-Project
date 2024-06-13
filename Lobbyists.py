import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Lobbyist:
    FilerId: int
    PersonId: int
    EthicsCourseCompleted: datetime

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all lobbyists
lobbyists = []
cursor.execute("""
               SELECT DISTINCT FILERNAME_CD.FILER_ID, ETHICS_DATE
               FROM FILERNAME_CD
               JOIN FILER_ETHICS_CLASS_CD ON FILERNAME_CD.FILER_ID = FILER_ETHICS_CLASS_CD.FILER_ID
               WHERE FILER_TYPE = "LOBBYIST"
               """)
db_lobbyists = cursor.fetchall()
print("Unique Lobbyists: ", cursor.rowcount)
for lobbyist in db_lobbyists:
    lobbyists.append(
        Lobbyist(
            FilerId=lobbyist[0],
            PersonId=0,  
            EthicsCourseCompleted=lobbyist[1]
        )
    )
cursor.close()
connection.close()

# # Connect to DDDB to check for persons in the Persons table
# connection = db_connect(os.getenv('DDDB'))
# cursor = connection.cursor()
# # Try to match into DDDB to get pid
# for person in persons:
#     select_query = """
#                    SELECT distinct pid
#                    FROM Person
#                    WHERE first = %s AND last = %s AND suffix = %s
#                    """
#     cursor.execute(select_query, (person.FirstName, person.LastName, person.Suffix))
#     result = cursor.fetchone()
    
#     if result:
#         person.PersonId = result[0]

# persons = [person for person in persons if person.PersonId != 0]
# print("Person count: ", len(persons))

# cursor.close()
# connection.close()

# insert Lobbyists into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT IGNORE INTO Lobbyists (FilerId, PersonId, EthicsCourseCompleted)
                VALUES (%s, %s, %s)
                """

data_to_insert = [
    (lobbyist.FilerId, lobbyist.PersonId, lobbyist.EthicsCourseCompleted)
    for lobbyist in lobbyists
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
