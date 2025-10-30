# ingest_data.py
import os
from sodapy import Socrata
from pymongo import MongoClient

# --- Configuration ---
SOCRATA_DOMAIN = "data.kcmo.org"
SOCRATA_DATASET_ID = "d4px-6rwg"
# Your connection string will be securely passed by GitHub Actions
MONGO_CONNECTION_STRING = os.environ.get("MONGO_CONNECTION_STRING")
DB_NAME = "kc_311_db"
COLLECTION_NAME = "raw_requests"

def ingest_data():
    if not MONGO_CONNECTION_STRING:
        raise ValueError("MONGO_CONNECTION_STRING environment variable not set!")

    print("Connecting to MongoDB Atlas...")
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    print("Fetching data from Socrata API...")
    socrata_client = Socrata(SOCRATA_DOMAIN, None)
    results = socrata_client.get(SOCRATA_DATASET_ID, limit=10000)

    if not results:
        print("No data fetched. Exiting.")
        return

    print(f"Fetched {len(results)} records. Deleting old raw data...")
    # Clear the collection before inserting new data
    collection.delete_many({})

    print(f"Inserting {len(results)} new records into '{COLLECTION_NAME}'...")
    collection.insert_many(results)

    print("Ingestion complete.")
    client.close()

if __name__ == "__main__":
    ingest_data()
