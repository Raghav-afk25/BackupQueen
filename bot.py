import time
from pymongo import MongoClient
import schedule
from datetime import datetime

SOURCE_MONGO_URI = "mongodb+srv://Ansh:Ansh@void.je4pzwn.mongodb.net/?retryWrites=true&w=majority"
DESTINATION_MONGO_URI = "mongodb+srv://void0286:l5S1IJIfebXTzJ50@cluster0.skcil.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def backup_data():
    """
    Backup all data from the source MongoDB server to the destination MongoDB server.
    """
    try:
        source_client = MongoClient(SOURCE_MONGO_URI)
        destination_client = MongoClient(DESTINATION_MONGO_URI)

        print(f"[{datetime.now()}] Starting backup...")

        # Iterate through databases and collections
        for db_name in source_client.list_database_names():
            if db_name in ["admin", "local", "config"]:  # Skip system databases
                continue

            print(f"Backing up database: {db_name}")
            source_db = source_client[db_name]
            destination_db = destination_client[db_name]

            for collection_name in source_db.list_collection_names():
                print(f"  Backing up collection: {collection_name}")
                source_collection = source_db[collection_name]
                destination_collection = destination_db[collection_name]

                destination_collection.delete_many({})

                documents = list(source_collection.find())
                if documents:
                    destination_collection.insert_many(documents)

        print(f"[{datetime.now()}] Backup completed successfully.")

    except Exception as e:
        print(f"[{datetime.now()}] Error during backup: {e}")

def main():
    """
    Main function to run the backup process immediately and schedule it hourly.
    """
    backup_data()

    schedule.every(1).hours.do(backup_data)

    print("Scheduled hourly backups. Press Ctrl+C to exit.")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
