import datetime
import os
from dotenv import load_dotenv
from db_connect import *
from dataclasses import dataclass

@dataclass
class LobbyingFirm:
    FilerId: int
    FirmName: str
    FirmCity: str
    FirmState: str
    FirmZip: str
    FirmAddress: str
    DateRegistered: datetime.date

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all lobbying firms
lobbying_firms = []
cursor.execute("""
               SELECT DISTINCT FILER_ID, NAML, CITY, ST, ZIP4, 
               CONCAT_WS(' ', ADR1, ADR2) AS Address, EFFECT_DT
               FROM FILERNAME_CD
               WHERE FILER_TYPE = "FIRM"
               """)
db_lobbying_firms = cursor.fetchall()
print("Unique Lobbying Firms: ", cursor.rowcount)
for firm in db_lobbying_firms:
    lobbying_firms.append(
        LobbyingFirm(
            FilerId=firm[0],
            FirmName=firm[1],
            FirmCity=firm[2],
            FirmState=firm[3],
            FirmZip=firm[4],
            FirmAddress=firm[5],
            DateRegistered=firm[6]
        )
    )
cursor.close()
connection.close()

# insert LobbyingFirms into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT IGNORE INTO LobbyingFirms (FilerId, FirmName, FirmCity, FirmState, FirmZip, FirmAddress, DateRegistered)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

data_to_insert = [
    (firm.FilerId, firm.FirmName, firm.FirmCity, firm.FirmState, firm.FirmZip, firm.FirmAddress, firm.DateRegistered)
    for firm in lobbying_firms
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
