import datetime
import os
from dotenv import load_dotenv

from db_connect import *

from dataclasses import dataclass

@dataclass
class Bill:
    bid: str
    type: str
    number: str
    billState: str
    status: str
    house: str
    session: int
    state: str
    lastTouched: datetime
    sessionYear: int
    dr_id: int
    lastTouched_ts: int
    visibility_flag: int
    dr_changed: datetime
    dr_changed_ts: int

load_dotenv()

connection = db_connect(
    os.getenv('DDDB'), 
    os.getenv('DB_USERNAME'),
    os.getenv('DB_PASSWORD'))

cursor = connection.cursor()

# get all Bills
bills = []
cursor.execute("SELECT * FROM Bill")
db_bills = cursor.fetchall()
print("Bill Count: ", cursor.rowcount)
for bill in db_bills:
    bills.append(
        Bill(
            bid=bill[0],
            type=bill[1],
            number=bill[2],
            billState=bill[3],
            status=bill[4],
            house=bill[5],
            session=bill[6],
            state=bill[7],
            lastTouched=bill[8],
            sessionYear=bill[9],
            dr_id=bill[10],
            lastTouched_ts=bill[11],
            visibility_flag=bill[12],
            dr_changed=bill[13],
            dr_changed_ts=bill[14],
        )
    )
    
for bill in bills:
    print(bill.bid)
