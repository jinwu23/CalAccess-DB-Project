import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Bill:
    BillId: int
    BillType: str
    BillNumber: str
    BillState: str
    BillStatus: str
    BillHouse: str
    BillSession: int
    BillUSState: str
    SessionYear: datetime
    BillTitle: str
    BillDigest: str

load_dotenv()

connection = db_connect(os.getenv('DDDB'))
cursor = connection.cursor()

# get all bills
bills = []
cursor.execute("""
               SELECT DISTINCT BillId, BillType, BillNumber, BillState, BillStatus, BillHouse, BillSession, BillUSState, SessionYear, BillTitle, BillDigest
               FROM (
                    SELECT 
                        Bill.bid AS BillId, 
                        Bill.type AS BillType, 
                        Bill.number AS BillNumber, 
                        Bill.billState AS BillState, 
                        Bill.status AS BillStatus, 
                        Bill.house AS BillHouse, 
                        Bill.session AS BillSession, 
                        Bill.state AS BillUSState, 
                        Bill.sessionYear AS SessionYear, 
                        BillVersion.title AS BillTitle, 
                        BillVersion.digest AS BillDigest,
                        ROW_NUMBER() OVER (PARTITION BY Bill.bid ORDER BY Bill.bid) AS rn
                    FROM 
                        Bill
                    JOIN 
                        BillVersion ON Bill.bid = BillVersion.bid
               ) AS ranked_bills 
               WHERE rn = 1;
               """)
db_bills = cursor.fetchall()
print("Bill Count: ", cursor.rowcount)
for bill in db_bills:
    bills.append(
        Bill(
            BillId= bill[0],
            BillType= bill[1],
            BillNumber= bill[2],
            BillState= bill[3],
            BillStatus= bill[4],
            BillHouse= bill[5],
            BillSession= bill[6],
            BillUSState= bill[7],
            SessionYear= bill[8],
            BillTitle= bill[9],
            BillDigest= bill[10]
        )
    )
cursor.close()
connection.close()

# inset bills into db
connection = db_connect(os.getenv('DEVDB'))
cursor = connection.cursor()

insert_query =  """
                INSERT INTO Bills (BillId, BillType, BillNumber, BillState, BillStatus, BillHouse, BillSession, BillUSState, SessionYear, BillTitle, BillDigest)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

data_to_insert = [
    (bill.BillId, bill.BillType, bill.BillNumber, bill.BillState, bill.BillStatus, bill.BillHouse, bill.BillSession, bill.BillUSState, bill.SessionYear, bill.BillTitle, bill.BillDigest)
    for bill in bills
]

batch_size = 1000
for i in range(0, len(data_to_insert), batch_size):
    batch_data = data_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch_data)
    connection.commit()

cursor.close()
connection.close()
