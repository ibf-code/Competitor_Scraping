import httpx
import json
import csv
import sqlite3
import os
import re
import asyncio
import traceback
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
from modules.stealth import get_random_user_agent 
from modules.auth import authenticate
from modules.scrape.mayesh import fetch_available_dates # used for earliest_eta

load_dotenv()


try:
    product_group_mapping = pd.read_csv('mapping/flowermarketplace_productgroups.csv', engine='python', on_bad_lines='skip')
    product_group_dict = {}
    for _, row in product_group_mapping.iterrows():
        if pd.notnull(row['ibf_product_group']) and pd.notnull(row['competitor_product_group']):
            key = str(row['ibf_product_group']).strip()
            value = str(row['competitor_product_group']).strip()
            if value.endswith(','): 
                value = value[:-1]
            product_group_dict[key] = value
    print(f"Successfully loaded {len(product_group_dict)} product group mappings")
except Exception as e:
    print(f"Error loading product group mapping: {e}")
    product_group_dict = {}

try:
    variety_mapping = pd.read_csv('mapping/flowermarketplace_varieties.csv')
    variety_mapping_dict = dict(zip(variety_mapping['competitor_variety'].astype(str),
                               variety_mapping['ibf_variety']))
    print(f"Successfully loaded {len(variety_mapping_dict)} variety mappings")
except Exception as e:
    print(f"Error loading variety mapping: {e}")
    variety_mapping_dict = {}

def get_ibf_product_group_id(product_group_name):
    # Try an exact match first
    result = product_group_dict.get(str(product_group_name))
    
    # If no exact match, try a case-insensitive partial match
    if not result or result == 'unmapped':
        for key, value in product_group_dict.items():
            if value != 'unmapped' and product_group_name.lower() in key.lower():
                return value
    
    return result

def get_ibf_variety(variety_id):
    return variety_mapping_dict.get(str(variety_id))

today = date.today().strftime("%Y-%m-%d")
async def process_page(client, page_number, product_key, eta_date):
    try:
        print(f"Processing page {page_number}...")
        formatted_date = datetime.strptime(eta_date, '%Y-%m-%d').strftime('%m/%d/%Y')
        url = f'https://flowermarketplace.com/wp-admin/admin-ajax.php?action=wpf_product_listings&model=landed&date_text={formatted_date}&page_no={page_number}'
        
        response = await client.get(url)
        
        # Check if the response is valid
        if response.status_code != 200:
            print(f"Error on page {page_number}: HTTP status {response.status_code}")
            return False
            
        # Try to parse the JSON response
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON on page {page_number}: {e}")
            return None
            
        # Check if the response is empty or has an unexpected structure
        if not data:
            print(f"Empty response on page {page_number}")
            return None

        # Access products based on the structure
        products = []
        if product_key and product_key in data:
            products = data[product_key]
        elif isinstance(data, list):
            products = data
        else:
            # Try to find a list of products in the response
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    products = value
                    break
        
        if not products:
            print(f"No products found on page {page_number}")
            return None

        page_products = []
        product_count = 0

        today = date.today().strftime("%Y-%m-%d")
        for product in products:
            try:
                raw_date = product.get('date_text')
                parsed_date = None
                if raw_date:
                    try:
                        parsed_date = datetime.strptime(raw_date, '%m/%d/%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        print(f"Invalid date: {raw_date}")

                name = product.get('name', '').strip()
                stem_length_match = re.search(r'(\d{2,3})\s?CM$', name.upper())

                if stem_length_match:
                    stem_length = int(stem_length_match.group(1))
                    name = re.sub(r'\s?\d{2,3}\s?CM$', '', name, flags=re.IGNORECASE).strip()
                else:
                    stem_length = None

                catslug_raw = product.get('catslug', '')
                competitor_product_group_name = catslug_raw.replace('-', ' ').title()

                product_data = {
                    'created_at': today,
                    'eta_date': parsed_date or eta_date,  # Use eta_date if parsed_date is None
                    'competitor_product_id': product.get('id'),
                    'competitor': 'Flowermarketplace',
                    'variety_key': get_ibf_variety(str(product.get('id'))),
                    'product_group_key': get_ibf_product_group_id(competitor_product_group_name),
                    'competitor_product_name': name,
                    'stem_length': stem_length,
                    'stem_price': float(product.get('landed_price')) if product.get('landed_price') else 0,
                    'competitor_product_group_name': competitor_product_group_name,
                    'grower_country':product.get('source'),
                    'unit': product.get('unit')
                }
                page_products.append(product_data)
                product_count += 1
                
            except Exception as e:
                print(f"Error processing product {product.get('id', 'unknown')}: {str(e)}")
                traceback.print_exc()

        print(f"Page {page_number} processed with {product_count} products.")
        return page_products
        
    except Exception as e:
        print(f"Error processing page {page_number}: {str(e)}")
        traceback.print_exc()
        return None

async def main():
    # First get the eta_date from Mayesh
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    session, headers = authenticate(email, password)
    eta_date = fetch_available_dates(session, headers) if session else None

    if not eta_date:
        print("Failed to fetch eta_date from Mayesh. Using today's date instead.")
        eta_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Using eta_date from Mayesh: {eta_date}")
    
    # Set up database
    os.makedirs('output/flowermarketplace', exist_ok=True)
    
    headers = {
        "User-Agent": get_random_user_agent(),
        "Referer": "https://flowermarketplace.com/",
        "Origin": "https://flowermarketplace.com",
        "Cache-Control": "no-cache",
    }

    # Add cookies if they're required for authentication
    cookies = {}

    all_products = []

    async with httpx.AsyncClient(headers=headers, cookies=cookies, follow_redirects=True, timeout=30) as client:
        # First request to determine the structure
        formatted_date = datetime.strptime(eta_date, '%Y-%m-%d').strftime('%m/%d/%Y')
        response = await client.get(f'https://flowermarketplace.com/wp-admin/admin-ajax.php?action=wpf_product_listings&model=landed&date_text={formatted_date}&page_no=1')
        
        if response.status_code != 200:
            print(f"Failed to access the first page: HTTP {response.status_code}")
            return
            
        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Failed to parse the first page response as JSON")
            return

        # Determine the correct key for accessing product data
        product_key = None
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    product_key = key
                    print(f"Found product key: {product_key}")
                    break
        

        semaphore = asyncio.Semaphore(6)
        
        async def process_with_rate_limit(page):
            async with semaphore:
                result = await process_page(client, page, product_key, eta_date)
                await asyncio.sleep(0.5)
                return result
        page = 1
        while True:
            products = await process_with_rate_limit(page)
        
            if not products:
                print(f"No products found on {page}")
                break
            all_products.extend(products)
            page += 1

    
    if all_products:
        csv_file = f'output/flowermarketplace/flowermarketplace_inventory_{eta_date}.csv'
        fieldnames = set()
        for product in all_products:
            fieldnames.update(product.keys())

        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(all_products)
        print(f"Exported {len(all_products)} products to {csv_file}")
    else:
        print("No products found")

    print(f"processing complete for {eta_date}")
if __name__ == "__main__":
    asyncio.run(main())