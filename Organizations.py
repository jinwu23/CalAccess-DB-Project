import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Organization:
    FilerId: int
    OrgName: str
    OrgType: str

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all organizations
organizations = []
cursor.execute("""
               SELECT DISTINCT FILER_ID, NAML, FILER_TYPE
               FROM FILERNAME_CD
               WHERE FILER_TYPE IN ("EMPLOYER", "SLATE MAILER ORGANIZATIONS", "RECIPIENT COMMITTEE")
               """)
db_organizations = cursor.fetchall()
print("Unique Organizations: ", cursor.rowcount)
for org in db_organizations:
    organizations.append(
        Organization(
            FilerId=org[0],
            OrgName=org[1],
            OrgType=org[2]
        )
    )
cursor.close()
connection.close()

# insert Organizations into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT IGNORE INTO Organizations (FilerId, OrgName, OrgType)
                VALUES (%s, %s, %s)
                """

data_to_insert = [
    (org.FilerId, org.OrgName, org.OrgType)
    for org in organizations
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
