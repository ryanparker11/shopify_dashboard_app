# register_compliance_webhooks.py
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

# Your app credentials
SHOPIFY_API_KEY = os.environ["SHOPIFY_API_KEY"]
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]
APP_URL = os.environ["APP_URL"]

# Get shop and access token from your database
# For now, use your test store
SHOP = "test-auth-1-2.myshopify.com"

async def get_access_token():
    """Get access token from your database"""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    conn = psycopg2.connect("postgresql://shopadmin:xXHnJ0EgVRls46b_9yhc2DO@commerce-app-cluster.cluster-ch4k0ageqg5j.us-east-2.rds.amazonaws.com:5432/shopapp?sslmode=require", cursor_factory=RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT access_token FROM shopify.shops WHERE shop_domain = %s", (SHOP,))
    row = cur.fetchone()
    return row['access_token'] if row else None

async def register_compliance_webhooks():
    access_token = await get_access_token()
    
    if not access_token:
        print("❌ No access token found. Install app on test store first.")
        return
    
    webhooks = [
        {
            "topic": "customers/data_request",
            "address": f"{APP_URL}/webhooks/customers/data_request",
            "format": "json"
        },
        {
            "topic": "customers/redact",
            "address": f"{APP_URL}/webhooks/customers/redact",
            "format": "json"
        },
        {
            "topic": "shop/redact",
            "address": f"{APP_URL}/webhooks/shop/redact",
            "format": "json"
        }
    ]
    
    async with httpx.AsyncClient() as client:
        for webhook in webhooks:
            response = await client.post(
                f"https://{SHOP}/admin/api/2024-10/webhooks.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                },
                json={"webhook": webhook}
            )
            
            if response.status_code == 201:
                print(f"✅ Registered: {webhook['topic']}")
            elif response.status_code == 422:
                print(f"⚠️  Already exists: {webhook['topic']}")
            else:
                print(f"❌ Failed: {webhook['topic']} - {response.text}")

import asyncio
asyncio.run(register_compliance_webhooks())