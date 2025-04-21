# Helper to extract the productgroups and IDS from the index page

import httpx
import csv
from selectolax.parser import HTMLParser
import re
import asyncio

INDEX_URL = "https://shop.dvflora.com/cgi-bin/dv.sh/nfproduct-index?CustCode=DVGuest&DVWebKey=73282720.02458272&CatKey=00000003&DVCust=00589425&SideType=ECommerce&HeaderType=Ecommerce&ShowPrice=&cat-key=00000003"
OUTPUT_CSV = "./utils/Mapping_products/dvflora_productgroups.csv"

async def main():
    async with httpx.AsyncClient(timeout=15) as client:
        response  = await client.get(INDEX_URL)
        html = HTMLParser(response.text)

        rows = []

        # each flower is in a td list class
        for td in html.css("td.item_list"):
            a = td.css_first("a")
            if not a or "onclick" not in a.attributes:
                continue

            # extract the webcatkey from the url attribute placed after "WebCatKey="
            onclick = a.attributes["onclick"]
            match = re.search(r"WebCatKey=(\d+)", onclick)
            if not match:
                continue

            webcatkey = match.group(1)

            # extract the name of the productgroup
            name_node = a.css_first("b")
            if not name_node:
                font = a.css_first("font") # fallback as not all flowers have a bold tag
                name = font.text(strip=True) if font else "Unknown"
            else:
                name = name_node.text(strip=True)

            rows.append((webcatkey, name))
        
        with open(OUTPUT_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["competitor_product_group_id", "competitor_product_group_name"])
            writer.writerows(rows)

if __name__ == "__main__":
    asyncio.run(main())



