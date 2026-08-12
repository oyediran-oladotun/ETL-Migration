import os
from pathlib import Path
from dotenv import load_dotenv
import pymssql

# declaring the path and allowing load_dotenv to load from .env file
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


def connect_to_mssql():
    connection = None            #this declares that no connections exists yet
    try:
        connection = pymssql.connect(
            server = os.getenv("DEST_HOST"),
            port = os.getenv("DEST_PORT"),
            user= os.getenv("DEST_USER"),
            password = os.getenv("DEST_PASSWORD"),
            database = os.getenv("DEST_DATABASE"),
        )

        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION;")
        record = cursor.fetchone()
        print("Connected to MSSQL successfully.")
        print("Server version:", record[0])
        cursor.close()


    except Exception as e:
        print(f"Error whle connecting to MSSQL: {e}")

    finally:
        if connection:
            connection.close()
            print("MSSQL connection is closed.")

if __name__ == "__main__":
   connect_to_mssql()