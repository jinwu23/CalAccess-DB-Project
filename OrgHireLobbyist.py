import datetime
import os
from dotenv import load_dotenv
from db_connect import *
from dataclasses import dataclass

@dataclass
class HireFirm:
    OrgId: int
    FirmId: int
    StartDate: str
    EndDate: str

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all hired firms
firm_hires = []
cursor.execute("""
               WITH LobbyistHire AS (
               SELECT DISTINCT FILER_ID, EFF_DATE, FORM_TYPE
               FROM CVR_REGISTRATION_CD
               WHERE FORM_TYPE = 'F601' 
                   AND FILER_ID IS NOT NULL 
                   AND EFF_DATE IS NOT NULL
                   AND ENTITY_CD = 'LBY'
               ),
               OrgIdTable AS (
                SELECT DISTINCT
                    organization.FILER_ID as OrgId,
                    SUB_NAME,
                    LEMP_CD.EFF_DATE AS StartDate,
                    LEMP_CD.CON_PERIOD AS EndDate
                FROM 
                    LEMP_CD
                LEFT JOIN 
                    FilerIdTable as organization ON organization.NAML = LEMP_CD.CLI_NAML
                WHERE LEMP_CD.CLI_NAML != '' AND LEMP_CD.SUB_NAME != ''
               )
               SELECT DISTINCT
                    OrgId,
                    lobbyingfirm.FILER_ID as FirmId,
                    StartDate,
                    EndDate
                FROM 
                    OrgIdTable
                LEFT JOIN 
                    FilerIdTable as lobbyingfirm ON lobbyingfirm.NAML = SUB_NAME
                WHERE OrgId IS NOT NULL AND lobbyingfirm.FILER_ID IS NOT NULL
               """)
db_firm_hires = cursor.fetchall()
print("Unique Firm Hires: ", cursor.rowcount)
for hire in db_firm_hires:
    firm_hires.append(
        HireFirm(
            OrgId=hire[0],
            FirmId=hire[1],
            StartDate=hire[2],
            EndDate=hire[3]
        )
    )
cursor.close()
connection.close()

# insert HireFirms into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT IGNORE INTO HireFirm (OrgId, FirmId, StartDate, EndDate)
                VALUES (%s, %s, %s, %s)
                """

data_to_insert = [
    (hire.OrgId, hire.FirmId, hire.StartDate, hire.EndDate)
    for hire in firm_hires
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
