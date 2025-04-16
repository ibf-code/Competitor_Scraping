import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import datetime
from modules.latest_eta_date import (
    get_latest_eta_date_flowermarketplace,
    get_latest_eta_date_mayesh,
    get_latest_eta_date_petaljet
)

load_dotenv()

credentials = service_account.Credentials.from_service_account_file('config/service_key.json')

project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")

def upload_flowermarketplace_to_bigquery():
    latest_file_path = get_latest_eta_date_flowermarketplace("./output/flowermarketplace")
    data = pd.read_csv(latest_file_path)
    data.columns = [col.strip().replace(" ", "_").replace(",", "").lower() for col in data.columns]
    table_id_flowermarketplace = os.getenv("TABLE_ID_FLOWERMARKETPLACE")
    table_full_id_flowermarketplace = f"{project_id}.{dataset_id}.{table_id_flowermarketplace}"
    pandas_gbq.to_gbq(
        data,  
        table_full_id_flowermarketplace, 
        project_id=project_id,  
        credentials=credentials,
        if_exists="append",  
        chunksize=1000, 
    )
    print(f"✅ Data with date {latest_file_path} for flowermarketplace successfully uploaded to BigQuery")
        
def upload_petaljet_to_bigquery():
    latest_file_path = get_latest_eta_date_petaljet("./output/petaljet")
    data = pd.read_csv(latest_file_path)
    data.columns = [col.strip().replace(" ", "_").replace(",", "").lower() for col in data.columns]
    table_id_petaljet = os.getenv("TABLE_ID_PETALJET")
    table_full_id_petaljet = f"{project_id}.{dataset_id}.{table_id_petaljet}"
    pandas_gbq.to_gbq(
        data,  
        table_full_id_petaljet, 
        project_id=project_id,  
        credentials=credentials,
        if_exists="append",  
        chunksize=1000, 
    )
    print(f"✅ Data with date {latest_file_path} for petaljet successfully uploaded to BigQuery")

def upload_mayesh_to_bigquery():
    latest_file_path = get_latest_eta_date_mayesh("./output/mayesh")
    data = pd.read_csv(latest_file_path)
    data.columns = [col.strip().replace(" ", "_").replace(",", "").lower() for col in data.columns]
    table_id_mayesh = os.getenv("TABLE_ID_MAYESH")
    table_full_id_mayesh = f"{project_id}.{dataset_id}.{table_id_mayesh}"
    pandas_gbq.to_gbq(
        data,
        table_full_id_mayesh,
        project_id=project_id,
        credentials=credentials,
        if_exists="append",
        chunksize=1000,
    )
    print(f"✅ Data with date {latest_file_path} for mayesh successfully uploaded to BigQuery")
