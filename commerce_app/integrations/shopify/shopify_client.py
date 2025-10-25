import os, httpx, asyncio
from dotenv import load_dotenv
from typing import AsyncIterator, Dict, List, Optional, Tuple
load_dotenv()

BASE = f"{os.environ['SHOP_URL']}/admin/api/{os.environ.get('API_VERSION','2024-10')}"
HEADERS = {
    "X-Shopify-Access-Token": os.environ["ADMIN_ACCESS_TOKEN"],
    "Content-Type": "application/json"
}

async def get_orders(limit=10):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE}/orders.json", params={"limit": limit}, headers=HEADERS)
        r.raise_for_status()
        return r.json()["orders"]

async def get_customers(limit=10):
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{BASE}/customers.json", params={"limit": limit}, headers=HEADERS)
        r.raise_for_status()
        return r.json()["customers"]
