from datetime import datetime
import pandas as pd

#Reads Productgroups.csv to map their productgroup id's on our productgroup KEY
product_group_mapping = pd.read_csv("mapping/productgroups.csv")
mapping_dict = dict(zip(product_group_mapping['competitor_product_group'].astype(str), 
                       product_group_mapping['ibf_product_group']))

def get_ibf_product_group(category_id):
    return mapping_dict.get(str(category_id))
  
#Reads Varieties.csv to map their Variety id's on our Variety _KEYS
variety_mapping = pd.read_csv("mapping/varieties.csv")
variety_mapping_dict = dict(zip(variety_mapping['competitor_variety'].astype(str),
                                variety_mapping['ibf_variety']))

def get_ibf_variety(variety_name):
    return variety_mapping_dict.get(str(variety_name))

def extract_stem_length(grade_name):
    if not grade_name:
        return None
    import re
    numbers = re.findall(r'\d+', str(grade_name))
    return int(numbers[0]) if numbers else None

def process_inventory_data(products, date):
    extracted_data = []
    today = datetime.now().strftime('%Y-%m-%d')

    for product in products:
        mayesh_category_id = product.get("category_id", 0)
        ibf_product_group = get_ibf_product_group(mayesh_category_id)
        mayesh_variety = product.get("product_id", 0)
        ibf_variety = get_ibf_variety(mayesh_variety)

        extracted_data.append({
            "created_at": today,
            "eta_date": date,
            "state": "Kentucky", #Needed as they apply different pricing per region  
            "competitor": "Mayesh",
            "grower_name": product.get("farm_name", "Unknown") if product.get("farm_name") else None,
            "grower_country": product.get("country_name", None),
            "competitor_product_id": product.get("product_id", 0),
            "competitor_product_name": product["name"],
            "competitor_product_group_name": product.get("category_name", None),
            "competitor_product_group_id": product.get("category_id", 0),
            "product_group_key": ibf_product_group,
            # "variety": product.get("variety_name", None), 
            "variety_key": ibf_variety,
            "stem_length": extract_stem_length(product.get("grade_name")),  
            "color_name": product.get("color_name", None),
            "competitor_product_url": f"https://www.mayesh.com/{product['seo_url']}",
            "competitor_product_image": f"https://www.mayesh.com{product['image']}",
            "available_units": product["qty"],
            "stems_per_unit": product.get("unit_count", None), 
            "stem_price": float(product['price_per_stem']) if product.get("price_per_stem") else None,
            "unit_price": float(product['price_per_unit']) if product.get("price_per_unit") else 0,
            "base_price": product.get("main_landed_cost", 0),
            "freight_price": product.get("freight", 0),
            "margin": (1 - (1 / product["markup"])) * 100 if product.get("markup") else 0,
        })

    print(f"✅ Processed {len(products)} products for {date}")
    return extracted_data
