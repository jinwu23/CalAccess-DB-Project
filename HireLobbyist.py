import datetime
import os
from dotenv import load_dotenv
from db_connect import *
from dataclasses import dataclass
import re

@dataclass
class HireLobbyist:
    OrgId: int
    LobbyistId: int
    StartDate: str
    EndDate: str
    DisclosureForm: str
    type: str

load_dotenv()

connection = db_connect(os.getenv('CALACCESSDB'))
cursor = connection.cursor()

# get all hired lobbyists
lobbyist_hires = []
cursor.execute("""
               SELECT DISTINCT SENDER_ID, FILER_ID, FROM_DATE, THRU_DATE, FORM_TYPE
               FROM CVR_LOBBY_DISCLOSURE_CD
               WHERE ENTITY_CD = 'LBY'
               """)
db_lobbyist_hires = cursor.fetchall()
print("Unique Lobbyist Hires: ", cursor.rowcount)
for hire in db_lobbyist_hires:
    orgId = hire[0]
    filerId = hire[1]

    try:
        # organization is a lobbying firm
        if orgId.startswith('F'):
            parsedOrgId = int(re.sub(r'\D', '', orgId))  # Remove non-digit characters
            parsedFilerId = int(re.sub(r'\D', '', filerId))  # Remove non-digit characters
            lobbyist_hires.append(
                HireLobbyist(
                    OrgId=parsedOrgId,
                    LobbyistId=parsedFilerId,
                    StartDate=hire[2],
                    EndDate=hire[3],
                    DisclosureForm=hire[4],
                    type="Lobbying Firm"
                )
            )
        # organization is a normal organization
        elif orgId.startswith('E'):
            parsedOrgId = int(re.sub(r'\D', '', orgId))  # Remove non-digit characters
            parsedFilerId = int(re.sub(r'\D', '', filerId))  # Remove non-digit characters
            lobbyist_hires.append(
                HireLobbyist(
                    OrgId=parsedOrgId,
                    LobbyistId=parsedFilerId,
                    StartDate=hire[2],
                    EndDate=hire[3],
                    DisclosureForm=hire[4],
                    type="Organization"
                )
            )
    except ValueError as e:
        print(f"Error parsing OrgId or FilerId for orgId: {orgId}, filerId: {filerId}. Error: {e}")
    
cursor.close()
connection.close()

# insert HireLobbyists into db
connection = db_connect(os.getenv('PRODDB'))
cursor = connection.cursor()

insert_firm_query =  """
                INSERT IGNORE INTO FirmHireLobbyist (FirmId, LobbyistId, StartDate, EndDate, DisclosureForm)
                VALUES (%s, %s, %s, %s, %s)
                """
insert_org_query =  """
                INSERT IGNORE INTO OrgHireLobbyist (OrgId, LobbyistId, StartDate, EndDate, DisclosureForm)
                VALUES (%s, %s, %s, %s, %s)
                """

firm_data_to_insert = [
    (hire.OrgId, hire.LobbyistId, hire.StartDate, hire.EndDate, hire.DisclosureForm)
    for hire in lobbyist_hires if hire.type == "Lobbying Firm"
]

org_data_to_insert = [
    (hire.OrgId, hire.LobbyistId, hire.StartDate, hire.EndDate, hire.DisclosureForm)
    for hire in lobbyist_hires if hire.type == "Organization"
]

batch_size = 1000

# Insert into FirmHireLobbyist
for i in range(0, len(firm_data_to_insert), batch_size):
    batch_data = firm_data_to_insert[i:i + batch_size]
    cursor.executemany(insert_firm_query, batch_data)
    connection.commit()

# Insert into OrgHireLobbyist
for i in range(0, len(org_data_to_insert), batch_size):
    batch_data = org_data_to_insert[i:i + batch_size]
    cursor.executemany(insert_org_query, batch_data)
    connection.commit()

cursor.close()
connection.close()

    connection.commit()

cursor.close()
connection.close()
