"""
# This script constructs a list of urls to bypass messy pagination 
# The reason we have to do this is because the pagination is done by a set of random product id located on the next page
# Example of "page 2" is https://shop.dvflora.com/cgi-bin/dv.sh/nfprod-list.w?WebCatKey=00000292&CustCode=DVGuest&ReqDate=04/21/25&DisplayType=list&NumProds=50&CatKey=00000003&cat-key=00000003&ip-prod-ct=51&Variety=ALL&Color=All&SideType=ECommerce&DVWebKey=73282720.02458272&PrevNext=NEXT&ProductList=00134184,00058824,00186793,00070347,00016790,00159382,00189408,00185772,00160010,00135393,00189634,00093222,00181977,00165976,00186791,00186795,00093251,00175704,00176772,00173661,00039919,00181187,00021937,00048508,00086873,00098391,00189424,00183627,00189425,00182726,00177792,00182810,00086881,00183626,00015451,00116495,00015911,00178501,00015851,00134165,00022306,00188396,00022775,00188299,00015878,00015810,00015935,00015459,00186773,00174827,&ByDate=&HeaderType=Ecommerce&MainContent=nfprod-list.w&NonStock=&ShowPrice=true
therefore we create links and finetune it to variety and color so that we stay in the (hard)limit of 50 products per page by webspeed (frontend).

# """

import csv
import asyncio 
import httpx
from selectolax.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from tqdm import tqdm

BASE_URL = "https://shop.dvflora.com/cgi-bin/dv.sh/nfprod-list.w"
COMMON_PARAMS = {
    "CustCode": "DVGuest",
    "DVWebKey": "73282720.02458272",
    "DVCust": "00589425",
    "DisplayType": "list",
    "NumProds": "50",
    "CatKey": "00000003", # Main category: flowers
    "cat-key": "00000003",
    "ShowPrice": "true", #
    "Image": "No", # Reduces the size of pages, increases speed of scraper
    "ip-prod-ct": "0",
    # "ByDate": "", # notice that we construct our urls without date this is because we still need to map the products to IBF ids. If we construct urls with dates we might exclude products that are not available yet causing them not to be mapped. However we can use this paramater later on to show prices per ETA date

}
INPUT_CSV = "./utils/Mapping_products/dvflora_categories.csv"
OUTPUT_CSV = "./utils/Mapping_products/dvflora_urls.csv"


# fetch raw HTML content of a  url using httpx.AsyncClient and return it as a string.
async def fetch_html(client, url):
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f" failed to fetch {url}: {e}")
        return ""

# extract a specific parameter value from a URL query string
def extract_param_from_url(url: str, key: str) -> str:
    try: 
        parsed = urlparse(url)
        return parse_qs(parsed.query).get(key, [None])[0]
    except Exception:
        return None

# parse the HTML content to extract varieties from a toggle select element
def parse_varieties(html):
    tree = HTMLParser(html)
    select = tree.css_first('select[name="Variety"]')
    if not select:
        return []
    values = []
    for option in select.css("option"):
        val = option.attrs.get("value", "").strip()
        if val.lower() == "all":
            continue
        if "Variety=" in val:
            variety = extract_param_from_url(val, "Variety")
        else:
            variety = option.text(strip=True)
        if variety and variety.lower() != "all":
            values.append(variety)
    return list(set(values))

# extract colors from a toggle select element
async def extract_colors(client, webcatkey, variety):
    url = f"{BASE_URL}?WebCatKey={webcatkey}&Variety={variety}"
    html = await fetch_html(client, url)
    tree = HTMLParser(html)
    select = tree.css_first('select[name="Color"]')
    if not select:
        return []
    values = []
    for option in select.css("option"):
        val = option.attrs.get("value", "").strip()
        if val.lower() == "all":
            continue
        if "Color=" in val:
            color = extract_param_from_url(val, "Color")
        else:
            color = option.text(strip=True)
        if color and color.lower() != "all":
            values.append(color)
    return list(set(values))


# main function to collect URLs from the input CSV file
async def collect_urls():
    tasks = []

    async with httpx.AsyncClient() as client:
        with open(INPUT_CSV, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = [row for row in reader if len(row) >= 2]



            # goes through rows in the CSV file and construct URLs for each product group
            for row in tqdm(rows, desc="Collecting URLs", unit="productgroup"):
                group_name, webcatkey = row[0].strip(), row[1].strip()
                variety_url = f"{BASE_URL}?WebCatKey={webcatkey}"
                html = await fetch_html(client, variety_url)
                varieties = parse_varieties(html)

                # if no varieties are found, skip this product group
                for variety in varieties:
                    colors = await extract_colors(client, webcatkey, variety)
                    if not colors:
                        colors = ["all"]

                    # construct URLs for each combination of product group, webcatkey, variety, and color
                    for color in colors:
                        params = COMMON_PARAMS.copy()
                        params.update({
                            "WebCatKey": webcatkey,
                            "Variety": variety,
                            "Color": color,
                        })
                        # Construct the full URL with query parameters
                        full_url = f"{BASE_URL}?{urlencode(params)}"
                        tasks.append((group_name, webcatkey, variety, color, full_url))

    # write the collected URLs to a CSV file
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_group", "webcatkey", "variety", "color", "url"])
        writer.writerows(tasks)
    print(f"✅ Collected {len(tasks)} URLs and saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(collect_urls())


