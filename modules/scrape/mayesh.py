import requests

DATES_URL = "https://www.mayesh.com/api/auth/dates"
INVENTORY_URL = "https://www.mayesh.com/api/auth/inventory"

def fetch_available_dates(session, headers):
    response  = session.post(DATES_URL, json={}, headers=headers)

    if response.status_code in [200, 201]:
        dates_data = response.json()

        farm_direct_dates = [
            entry["delivery_date"] for entry in dates_data.get("dates", [])
            if entry["program_id"] == 5 # Program ID for Farm Direct
        ]

        if not farm_direct_dates:
            print("💀 No Farm Direct dates available.")
            return None
        
        min_delivery_date = min(farm_direct_dates)
        print(f"🗓️ Using delivery date {min_delivery_date}")
        return min_delivery_date
    
    else:
        print(f"something went wrong with fetching dates{response.status_code}")
        return None

def fetch_inventory(session, headers, delivery_date):
    print(f" Collecting inventory for {delivery_date}")
    payload = {
        "filters": {
            "perPage": 2000,
            "sortBy": "Name-ASC",
            "pageNumb": 1,
            "date": delivery_date,
            "is_sales_rep": 0,
            "is_e_sales": 0,
            "criteria": {"filter_program": ["5"]},
            "criteriaInt": {"filter_program": {"5": "Farm Direct Boxlots"}},
            "search": ""
        }
    }

    response = session.post(INVENTORY_URL, json=payload, headers=headers)
    if response.status_code in [200, 201]:
        data = response.json()
        print(f"✅ fetched inventory for {delivery_date}, found {len(data.get('products', []))} products")
        return data.get("products", [])
    else:
        print(f"yikes couldn't fetch inventory for {delivery_date}. status code: {response.status_code}")
        return []
    
