import asyncio
import csv
import re
from pathlib import Path
import aiohttp # faster compared to httpx
import aiosqlite # only in testin environment
from selectolax.parser import HTMLParser
from tqdm import tqdm # to show progress bar

URLS_CSV = Path("./utils/Mapping_products/dvflora_urls.csv")
DB_PATH = Path("./utils/Mapping_products/dvflora.db") # while testing this is prevered and cleaner
CONCURRENCY = 100 # the higher the faster, yet can be unstable
BATCH_SIZE = 500 # used during testing to increase speed batch append to db
sem = asyncio.Semaphore(CONCURRENCY) # limit the number of concurrent requests

# used during testing to inspect results in a clean db
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS products (
    item_number TEXT PRIMARY KEY,
    product_name TEXT,
    origin TEXT,
    sold_as TEXT,
    in_stock BOOLEAN,
    unit_price REAL,
    product_group TEXT,
    variety TEXT,
    color TEXT,
    url TEXT
)
"""
# see comment in the function above
INSERT_SQL = """
INSERT OR IGNORE INTO products 
(item_number, product_name, origin, sold_as, in_stock, unit_price,
 product_group, variety, color, url)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def extract_products(html: str, group: str, variety: str, color: str, url: str):
    tree = HTMLParser(html)
    tbody = tree.css_first("#PageText > form:nth-of-type(2) > table > tbody")
    if not tbody:
        return []

    products = []
    seen = set()

    for row in tbody.css("tr"):
        cells = row.css("td")
        if len(cells) < 8:
            continue

        item_number = cells[1].text(strip=True)
        if not item_number or item_number in seen:
            continue
        seen.add(item_number)

        name_node = cells[2].css_first("a")
        product_name = name_node.text(strip=True) if name_node else None

        origin_text = cells[2].text()
        origin = origin_text.split("Origin:")[-1].strip() if "Origin:" in origin_text else None
        sold_as = cells[3].text(strip=True)

        stock_img = cells[4].css_first("img")
        in_stock = "yes" in stock_img.attrs.get("src", "").lower() if stock_img else False

        # Search all cells for any visible price
        unit_price = None
        for td in cells:
            text = td.text(strip=True)
            match_qty_price = re.search(r"(\d+)\s*@\s*\$?(\d+\.\d+)", text)
            match_price_only = re.search(r"\$?(\d+\.\d+)", text)
            if match_qty_price:
                unit_price = float(match_qty_price.group(2))
                break
            elif match_price_only:
                unit_price = float(match_price_only.group(1))
                break

        if product_name and item_number:
            products.append((
                item_number, product_name, origin, sold_as, in_stock,
                unit_price, group, variety, color, url
            ))

    return products

async def fetch_and_extract(session, row):
    async with sem:
        try:
            async with session.get(row["url"], timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                return extract_products(html, row["product_group"], row["variety"], row["color"], row["url"])
        except Exception as e:
            print(f"Error fetching {row['url']}: {e}")
            return []
        
async def insert_all_batches(conn, data, batch_size=BATCH_SIZE):
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        await conn.executemany(INSERT_SQL, batch)
    await conn.commit()

async def scrape_all():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(CREATE_SQL)
        await conn.commit()

        with open(URLS_CSV, newline="") as f:
            urls = list(csv.DictReader(f))

        connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": "Mozilla/5.0"}
        ) as session:
            tasks = [fetch_and_extract(session, row) for row in urls]
            all_results = []
            for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="⚡ Scraping"):
                result = await coro
                all_results.extend(result)

        await insert_all_batches(conn, all_results)

    print(f"\n✅ Scraped and inserted into {DB_PATH}: {len(all_results)} rows.")

if __name__ == "__main__":
    asyncio.run(scrape_all())