import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Person:
    FilerId: int
    PersonId: int
    FirstName: str
    LastName: str
    Suffix: str
    Prefix: str

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all filers
persons = []
cursor.execute("""
               SELECT DISTINCT FILER_ID, NAML, NAMF, NAMT, NAMS
               FROM FILERNAME_CD
               WHERE FILER_TYPE IN ("INDIVIDUAL", "LOBBYIST")
               """)
db_persons = cursor.fetchall()
print("Unique Persons: ", cursor.rowcount)
for person in db_persons:
    persons.append(
        Person(
            FilerId=person[0],
            PersonId=0,  
            LastName=person[1],
            FirstName=person[2],
            Prefix=person[3],
            Suffix=person[4]
        )
    )
cursor.close()
connection.close()

# Connect to DDDB to check for persons in the Persons table
connection = db_connect(os.getenv('DDDB'))
cursor = connection.cursor()

# Try to match into DDDB to get pid
for person in persons:
    select_query = """
                   SELECT distinct pid
                   FROM Person
                   WHERE first = %s AND last = %s AND suffix = %s
                   """
    cursor.execute(select_query, (person.FirstName, person.LastName, person.Suffix))
    result = cursor.fetchone()
    
    if result:
        person.PersonId = result[0]

persons = [person for person in persons if person.PersonId != 0]
print("Person count: ", len(persons))

cursor.close()
connection.close()

# # inset filerIds into db
# connection = db_connect(os.getenv('DEVDB'))
# cursor = connection.cursor()

# insert_query =  """
#                 INSERT INTO Filer (FilerId)
#                 VALUES (%s)
#                 """

# data_to_insert = [
#     (filerId,)
#     for filerId in filerIds
# ]

# batch_size = 1000
# for i in range(0, len(data_to_insert), batch_size):
#     batch_data = data_to_insert[i:i + batch_size]
#     cursor.executemany(insert_query, batch_data)
#     connection.commit()

# cursor.close()
# connection.close()
