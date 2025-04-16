import pandas as pd
from datetime import datetime
import re
from pathlib import Path
# import os

MAYESH_INVENTORY = "mayesh_inventory_"
FLOWERMARKETPLACE_INVENTORY = "flowermarketplace_inventory_"
PETALJET_INVENTORY = "petaljet_inventory_"


def get_latest_eta_date_mayesh(directory: str, prefix: str = MAYESH_INVENTORY) -> Path:
    folder = Path(directory)
    date_pattern = re.compile(rf"{prefix}(\d{{4}}-\d{{2}}-\d{{2}})\.csv")
    date_files = []
    for file in folder.glob(f"{prefix}*.csv"):
        match = date_pattern.match(file.name)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            date_files.append((file_date, file))
        
    if not date_files:
        raise FileNotFoundError(f"No files found in {directory} with prefix {prefix}")
        
    latest_eta = max(date_files, key=lambda x: x[0])[1]
    return latest_eta

def get_latest_eta_date_petaljet(directory: str, prefix: str = PETALJET_INVENTORY) -> Path:
    folder = Path(directory)
    date_pattern = re.compile(rf"{prefix}(\d{{4}}-\d{{2}}-\d{{2}})\.csv")
    date_files = []
    for file in folder.glob(f"{prefix}*.csv"):
        match = date_pattern.match(file.name)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            date_files.append((file_date, file))
        
    if not date_files:
        raise FileNotFoundError(f"No files found in {directory} with prefix {prefix}")
        
    latest_eta = max(date_files, key=lambda x: x[0])[1]
    return latest_eta   

def get_latest_eta_date_flowermarketplace(directory: str, prefix: str = FLOWERMARKETPLACE_INVENTORY) -> Path:
    folder = Path(directory)
    date_pattern = re.compile(rf"{prefix}(\d{{4}}-\d{{2}}-\d{{2}})\.csv")

    date_files = []
    for file in folder.glob(f"{prefix}*.csv"):
        match = date_pattern.match(file.name)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            date_files.append((file_date, file))
        
    if not date_files:
        raise FileNotFoundError(f"No files found in {directory} with prefix {prefix}")
        
    latest_eta = max(date_files, key=lambda x: x[0])[1]
    return latest_eta  

print(get_latest_eta_date_mayesh("./output/mayesh"))
print(get_latest_eta_date_flowermarketplace("./output/flowermarketplace"))
print(get_latest_eta_date_petaljet("./output/petaljet"))