# Legacy code for exporting data to BigQuery , replaced by export_to_bq.py refer to commit 4fc7a5e


import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()

credentials = service_account.Credentials.from_service_account_file('config/service_key.json')

project_id = "ibuyflower-dwh"
dataset_id = "competitor_prices"
table_id = os.getenv("TABLE_ID_PETALJET")
table_full_id = f"{project_id}.{dataset_id}.{table_id}"

data = pd.read_csv("output/petaljet/petaljet_inventory_2025-04-29.csv")
data.columns = [col.strip().replace(" ", "_").replace(",", "").lower() for col in data.columns]

pandas_gbq.to_gbq(
    data,  
    table_full_id, 
    project_id=project_id,  
    credentials=credentials,
    if_exists="append",  
    chunksize=1000, 
)

print("✅ Data successfully uploaded to BigQuery")