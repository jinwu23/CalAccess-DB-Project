import os
from dotenv import load_dotenv

import mysql.connector 

load_dotenv()

# db_connect takes a database name and returns the connection
def db_connect(database):
    db_server = os.getenv('DB_HOSTNAME')
    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')

    try:
        connection = mysql.connector.connect(
            host = db_server,
            database = database,
            user = db_username,
            password = db_password,
        )
    except mysql.connector.Error as e:
        print("Error connecting to Database", e)
    return connection
