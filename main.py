from modules.scrape.mayesh import fetch_available_dates, fetch_inventory
from modules.data import process_inventory_data
from modules.store import save_to_csv
from modules.stealth import random_delay, get_random_user_agent
from modules.auth import authenticate
from export.export_to_bq import upload_mayesh_to_bigquery
from dotenv import load_dotenv
import os

load_dotenv()

EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")

session, headers = authenticate(EMAIL, PASSWORD)

if session and headers:
    delivery_date = fetch_available_dates(session, headers)
    
    if delivery_date:
        headers["User-Agent"] = get_random_user_agent()
        random_delay()

        raw_inventory = fetch_inventory(session, headers, delivery_date)
        
        if raw_inventory:
            processed_inventory = process_inventory_data(raw_inventory, delivery_date)
            filename = f"mayesh_inventory_{delivery_date}.csv"
            save_to_csv(processed_inventory, filename, subdir="mayesh", output_root="output")

            csv_path = os.path.join("output", "mayesh", filename)
            try:
                upload_mayesh_to_bigquery()
                print(f"✅ Data with date {delivery_date} for Mayesh successfully uploaded to BigQuery")
            except Exception as e:
                print(f"Error uploading data to BigQuery: {e}")

