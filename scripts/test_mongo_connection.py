import os 
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

# Load the .env from the project root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path = env_path)


def connect_to_mongo():
    client = None
    try:
    # MongoClient takes the full connection URI string from .env
        client = MongoClient (os.getenv("MONGO_URI"))

    #Verify the connection by pinging the admin database
    # 'ping' is a tiny command that asks the server to confirm it's alive.

        client.admin.command("ping")
        print("Connected to MongoDB successfully.")

    # to confirm we are seeing the right server
        print("Database available:", client.list_database_names())
        return client

    except Exception as e:      # tells us ifan error occured
        print(f"Error while connectng to MongoDB: {e}")

    finally:
        if client:
            client.close()
            print("MongoDB connection is closed.")

# if __name__ == "__main__":
#     connect_to_mongo()

