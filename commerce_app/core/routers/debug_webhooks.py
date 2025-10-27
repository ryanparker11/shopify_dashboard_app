#!/usr/bin/env python3
"""
Manual Webhook Registration - Handles various DB formats
"""

import os
import requests
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

SHOP_DOMAIN = "dashboard-mvp.myshopify.com"
APP_URL = "https://api.lodestaranalytics.io"

print("=" * 70)
print("MANUAL WEBHOOK REGISTRATION")
print("=" * 70)

# Try to build connection string
print(f"\n🔍 Building database connection...")

# First, try DATABASE_URL if it exists
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    print("Using DATABASE_URL from environment")
    connection_string = DATABASE_URL
else:
    # Build from components
    DB_HOST = os.getenv("DB_HOST", "")
    DB_NAME = os.getenv("DB_NAME", "")
    DB_USER = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
    
    # Handle case where DB_HOST includes port or database
    if ":" in DB_HOST:
        # Format: host:port or host:port/database
        parts = DB_HOST.split(":")
        DB_HOST = parts[0]
        if "/" in parts[1]:
            # host:port/database
            port_and_db = parts[1].split("/")
            DB_PORT = port_and_db[0]
            if not DB_NAME:
                DB_NAME = port_and_db[1]
        else:
            # just host:port
            DB_PORT = parts[1]
    elif "/" in DB_HOST:
        # Format: host/database
        parts = DB_HOST.split("/")
        DB_HOST = parts[0]
        if not DB_NAME:
            DB_NAME = parts[1]
    
    print(f"Using components:")
    print(f"  Host: {DB_HOST}")
    print(f"  Port: {DB_PORT}")
    print(f"  Database: {DB_NAME}")
    print(f"  User: {DB_USER}")
    
    # Build PostgreSQL connection string
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"

# Get access token from database
print(f"\n🔍 Looking up {SHOP_DOMAIN} in database...")

try:
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute(
        "SELECT shop_id, access_token FROM shopify.shops WHERE shop_domain = %s",
        (SHOP_DOMAIN,)
    )
    
    shop = cur.fetchone()
    cur.close()
    conn.close()
    
    if not shop:
        print(f"❌ Shop {SHOP_DOMAIN} NOT found in database!")
        print("\n💡 This means the app was not installed successfully.")
        print("   Solution: Reinstall the app using the custom distribution link")
        exit(1)
    
    print(f"✅ Found shop (ID: {shop['shop_id']})")
    access_token = shop['access_token']
    
except Exception as e:
    print(f"❌ Database error: {e}")
    print("\n💡 Your .env should have either:")
    print("   DATABASE_URL=postgresql://user:pass@host:5432/dbname")
    print("\nOR separate variables:")
    print("   DB_HOST=your-rds-endpoint.rds.amazonaws.com")
    print("   DB_PORT=5432")
    print("   DB_NAME=shopapp")
    print("   DB_USER=postgres")
    print("   DB_PASSWORD=yourpassword")
    exit(1)

# Register webhooks
webhooks_to_create = [
    {"topic": "orders/create", "address": f"{APP_URL}/webhooks/ingest"},
    {"topic": "orders/updated", "address": f"{APP_URL}/webhooks/ingest"},
    {"topic": "products/create", "address": f"{APP_URL}/webhooks/ingest"},
    {"topic": "products/update", "address": f"{APP_URL}/webhooks/ingest"},
    {"topic": "customers/create", "address": f"{APP_URL}/webhooks/ingest"},
    {"topic": "customers/update", "address": f"{APP_URL}/webhooks/ingest"},
]

print(f"\n🔄 Registering {len(webhooks_to_create)} webhooks with Shopify...\n")

success_count = 0
already_exists_count = 0
error_count = 0

for webhook_config in webhooks_to_create:
    try:
        response = requests.post(
            f"https://{SHOP_DOMAIN}/admin/api/2024-10/webhooks.json",
            headers={
                "X-Shopify-Access-Token": access_token,
                "Content-Type": "application/json"
            },
            json={"webhook": webhook_config},
            timeout=10
        )
        
        if response.status_code == 201:
            print(f"✅ Registered: {webhook_config['topic']}")
            success_count += 1
        elif response.status_code == 422:
            error_data = response.json()
            if "already exists" in str(error_data).lower():
                print(f"⚠️  Already exists: {webhook_config['topic']}")
                already_exists_count += 1
            else:
                print(f"❌ Error: {webhook_config['topic']} - {error_data}")
                error_count += 1
        else:
            print(f"❌ Failed: {webhook_config['topic']} - Status {response.status_code}")
            print(f"   Response: {response.text}")
            error_count += 1
            
    except Exception as e:
        print(f"❌ Error registering {webhook_config['topic']}: {e}")
        error_count += 1

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Successfully registered: {success_count}")
print(f"⚠️  Already existed: {already_exists_count}")
print(f"❌ Errors: {error_count}")

if success_count > 0 or already_exists_count > 0:
    print("\n✅ WEBHOOKS ARE NOW ACTIVE!")
    print("\n📝 TEST IT NOW:")
    print("   1. Go to your Shopify admin")
    print("   2. Create a test order")
    print("   3. Wait 10 seconds")
    print(f"   4. Check: {APP_URL}/webhooks/status?shop_domain={SHOP_DOMAIN}")
    print("   5. The order should appear in your frontend!")
else:
    print("\n❌ No webhooks were registered!")
    print("   Check the errors above for details")




