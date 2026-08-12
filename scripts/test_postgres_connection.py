import os                   # reads into the environment (.env) variables
from pathlib import Path    # builds the path to the .env file
from dotenv import load_dotenv   # this does the load for the .env file
import psycopg2                  # the postgres driver connector

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv()

def connect_to_postgres():
    connection = None  #meaning no connection exists at first
    try:
        connection = psycopg2.connect(
            host = os.getenv("SDB_HOST"),
            port = int(os.getenv("SDB_PORT")),
            user = os.getenv("SDB_USER"),
            password = os.getenv("SDB_PASSWORD"),
            dbname = os.getenv("SDB_NAME"),
        )

        cursor = connection.cursor()            # cursor runs the query and holds resuls
        cursor.execute("SELECT version();")     # returns the server version
        record = cursor.fetchone()              # fetch the single row record
        print(f"Connected to PostgresSQL {record[0]}")
        cursor.close()                          # close the cursor
        return connection

    except Exception as e:
        print("Error while connecting to PostgresSQL: {e}")

    finally:
        if connection:
            connection.close()
            print("PostgresSQL connection is closed.")

if __name__ == "__main__":
    connect_to_postgres()