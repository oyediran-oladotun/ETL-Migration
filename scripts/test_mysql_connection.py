# --- Imports: the toolboxes this script needs ---

import os                              # lets us read environment variables (our .env values)
from pathlib import Path              # a clean, safe way to build file paths
from dotenv import load_dotenv        # loads the .env file so os.getenv can see our secrets
import mysql.connector                # the MySQL driver: lets Python talk to a MySQL database
from mysql.connector import Error     # the specific error type MySQL raises, so we can catch it



# --- Find and load the .env file ---

# __file__ is this script's own path. .resolve() makes it absolute (no surprises).
# .parent goes up to the scripts/ folder; the second .parent goes up to the project root.
# / ".env" points at the .env file sitting in that project root.
env_path = Path(__file__).resolve().parent.parent / ".env"

# Hand load_dotenv that exact path so it loads the right file no matter where we run from.
load_dotenv(dotenv_path=env_path)


# --- The work, wrapped in a function so it's reusable (and Airflow-ready later) ---

def connect_to_database():
    connection = None                  # define this up front so 'finally' can always check it

    try:
        # Open the connection to MySQL using the values from our .env file.
        # mysql.connector.connect makes the connection (API call) to the MySQL DB
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),                  # the server address       
            user=os.getenv("MYSQL_USER"),                  # the username
            password=os.getenv("MYSQL_PASSWORD"),          # the password
            database=os.getenv("MYSQL_DATABASE"),           # which database to use
            port=int(os.getenv("MYSQL_PORT")),             # the port, turned into a number

        )

        #the if statement is optional it is just a confirmation if the connection is successful using the print message;
        #is_connected() confirms the connection actually opened successfully.
        if connection.is_connected():
            db_info = connection.get_server_info()         # ask the server for its version text
            print(f"Connected to MySQL Server version {db_info}")

            cursor = connection.cursor()                   # a cursor runs queries and holds their results
            cursor.execute("SELECT DATABASE();")           # ask MySQL which database we're in
            record = cursor.fetchone()                     # fetch the single row that query returns
            print(f"You are currently connected to database: {record[0]}")  # record[0] = first column

            #return connection
            cursor.close()                                 # close the cursor now that we're done with it

    except Error as e:                                     # if anything MySQL-related goes wrong...
        print(f"Error while connecting to MySQL: {e}")     # ...show a readable message instead of crashing

    finally:                                               # this block runs no matter what (success or error)
        if connection and connection.is_connected():       # only if a live connection exists...
            connection.close()                             # ...close it, so we never leak open connections
            print("MySQL connection is closed.")


# --- Entry point: only runs when this file is executed directly ---

# When you run "python test_mysql_connection.py", Python sets __name__ to "__main__",
# so this block fires. If another script imports this file instead, it won't auto-run.
if __name__ == "__main__":
    connect_to_database()


# Postgres Connection
# driver installation
