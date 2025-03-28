import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()

credentials = service_account.Credentials.from_service_account_file('config/service_key.json')

project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("TABLE_ID")
table_full_id = f"{project_id}.{dataset_id}.{table_id}"

data = pd.read_csv("output/mayesh/mayesh_inventory_2025-04-08.csv")
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