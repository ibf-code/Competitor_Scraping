import asyncio
import csv
import json
import re
import datetime 
import httpx
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
import os
from modules.stealth import get_random_user_agent 
from modules.auth import authenticate
from modules.scrape.mayesh import fetch_available_dates # used for earliest_eta
from export.export_to_bq import upload_petaljet_to_bigquery # WIP

## upload_to_bq function to be inserted somewhere here

load_dotenv()

PAGES = [
    f"https://petaljet.com/collections/all-products?page={i}" for i in range(1, 16)
]

# make sure pages are loaded
TIMEOUT = 15

today = datetime.date.today().strftime("%Y-%m-%d")
OUTPUT_FILE = f"output/petaljet/PetalJet_inventory_{today}.csv"
HEADERS = {
    "User-Agent": get_random_user_agent(), 
    "Referer": "https://petaljet.com/",
    "Origin": "https://petaljet.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

# handles login for petaljet
shopify_cookie_value = os.getenv("SHOPIFY_ESSENTIAL_COOKIE")

COOKIES = {
    "_shopify_essential": shopify_cookie_value
}

async def fetch_page(client, url):
    try:
        resp = await client.get(url,headers=HEADERS, timeout=TIMEOUT)
        soup = BeautifulSoup(resp.text, "html.parser")
        for script in soup.find_all("script"):
            if script.string and '"products":[' in script.string:
                match = re.search(r"var meta = (\{.*?\});", script.string, re.DOTALL)
                if match:
                    return json.loads(match.group(1))["products"]
    except Exception as e:
        print(f"\u26a0 Error on {url}: {e}")
    return []

product_group_mapping = pd.read_csv("Mapping/petaljet_productgroups.csv")
mapping_dict = dict(zip(product_group_mapping['competitor_product_group'].astype(str), 
                       product_group_mapping['ibf_product_group']))
 
def get_ibf_product_group(product_type):
    return mapping_dict.get(str(product_type))

variety_mapping = pd.read_csv("Mapping/petaljet_varieties.csv")
variety_mapping_dict = dict(zip(variety_mapping['competitor_variety'].astype(str),
                                 variety_mapping['ibf_variety']))

def get_ibf_variety(variety_name):
    return variety_mapping_dict.get(str(variety_name))


def extract_variant_data(product, eta_date):
    variants = []
    today = datetime.date.today().strftime("%Y-%m-%d")
    for v in product.get("variants", []):
        full_name = v.get("name", "")
        petaljet_product_type = product.get("type", "")
        ibf_product_group = get_ibf_product_group(petaljet_product_type)
        petaljet_variant_id = v.get("id", 0)
        ibf_variety = get_ibf_variety(petaljet_variant_id)
        price = round(v.get("price", 0) / 100, 2)
        length_match = re.search(r"(\d{2})[cC][mM]", full_name)
        stems_match = re.search(r"(\d+)\s+Stems", full_name)
        price_per_match = re.search(r"\$(\d+\.\d{2})", full_name)
        stem_length = int(length_match.group(1)) if length_match else ""
        stems_each = int(stems_match.group(1)) if stems_match else ""
        price_per_stem = float(price_per_match.group(1)) if price_per_match else ""
        base_name = re.sub(r"-.*", "", full_name).strip()

        variants.append({
            "created_at": today,
            "eta_date": eta_date,
            "competitor_product_id": product.get("id"),
            "competitor": "PetalJet",
            "competitor_variant_id": v.get("id"),
            "product_group_key": ibf_product_group,
            "variety_key": ibf_variety,
            "competitor_product_group_name": product.get("type", ""),
            "competitor_product_name": base_name,
            "stem_length": stem_length,
            "stems_per_unit": stems_each,
            "unit_price": price,
            "stem_price": price_per_stem,
            "min_stems_each": stems_each,
        })
    return variants

async def main():
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    session, headers = authenticate(email, password)
    eta_date = fetch_available_dates(session, headers) if session else None

    if not eta_date:
        print("Failed to fetch eta_date from Mayesh. Exiting.")
        return
    
    print(f"Using eta_date from Mayesh: {eta_date}")
    all_items = []
    async with httpx.AsyncClient(headers=HEADERS, cookies=COOKIES, follow_redirects=True) as client:
        tasks = [fetch_page(client, url) for url in PAGES]
        results = await asyncio.gather(*tasks)
        for product_list in results:
            for product in product_list:
                all_items.extend(extract_variant_data(product, eta_date))

    df = pd.DataFrame(all_items)
    grouped = df.groupby(["competitor_product_name", "stem_length"])["stems_per_unit"]
    df["max_stems_each"] = grouped.transform("max")
    df.sort_values(by=["competitor_product_name", "stem_length", "stems_per_unit"], inplace=True)

    output_file = f"output/petaljet/PetalJet_inventory_{eta_date}.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ Scraped {len(df)} product variants to {output_file}")

    try:
        upload_petaljet_to_bigquery(output_file)
        print("✅ Data successfully uploaded to BigQuery")
    except Exception as e:
        print(f"❌ Failed to upload data to BigQuery: {e}")

if __name__ == "__main__":
    asyncio.run(main())