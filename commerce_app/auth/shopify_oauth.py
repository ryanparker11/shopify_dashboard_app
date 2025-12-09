import base64, hashlib, hmac, time, urllib.parse as urlparse
from typing import Dict, Optional
from fastapi import APIRouter, Request, Response, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import httpx, os, json, asyncio
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging


logger = logging.getLogger(__name__)
load_dotenv()

router = APIRouter(prefix="/auth", tags=["shopify-auth"])

SHOPIFY_API_KEY = os.environ["SHOPIFY_API_KEY"]
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]
APP_URL = os.environ["APP_URL"].rstrip("/")
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://app.lodestaranalytics.io")
SCOPES = (
    "read_customers,write_customers,read_fulfillments,write_fulfillments,"
    "write_inventory,read_inventory,read_orders,write_orders,read_products,write_products"
)
GRANT_PER_USER = os.environ.get("GRANT_OPTIONS_PER_USER", "false").lower() == "true"


def db():
    # Build from components instead of DATABASE_URL
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT", "5432")
    return psycopg2.connect(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require",
        cursor_factory=RealDictCursor,
    )


def is_valid_shop(shop: str) -> bool:
    return shop.endswith(".myshopify.com") and shop.count(".") >= 2 and "/" not in shop


def sign_hmac(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def verify_hmac(secret: str, query: Dict[str, str]) -> bool:
    q = {k: v for k, v in query.items() if k not in ("hmac", "signature")}
    pairs = [f"{k}={v}" for k, v in sorted(q.items(), key=lambda kv: kv[0])]
    msg = "&".join(pairs)
    computed = sign_hmac(secret, msg)
    provided = query.get("hmac", "")
    return hmac.compare_digest(computed, provided)


def set_cookie(response: Response, name: str, value: str, max_age: int = 300):
    response.set_cookie(
        name, value, max_age=max_age, httponly=True, secure=True, samesite="none"
    )


def get_cookie(request: Request, name: str) -> Optional[str]:
    return request.cookies.get(name)


# ============================================================================
# SYNC PROGRESS HELPERS
# ============================================================================
async def update_sync_progress(
    shop_id: int,
    stage: str,
    status: str = "in_progress",
    count: int = 0,
    error: str = None
):
    """
    Update sync progress in database.
    
    Args:
        shop_id: The shop's database ID
        stage: Current sync stage ('customers', 'products', 'orders', 'line_items')
        status: Status of the stage ('pending', 'in_progress', 'completed', 'failed')
        count: Number of items synced for this stage
        error: Error message if failed
    """
    from commerce_app.core.db import get_conn
    
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE shopify.shops 
                    SET 
                        sync_current_stage = %s,
                        sync_stage_status = %s,
                        sync_customers_count = CASE WHEN %s = 'customers' THEN %s ELSE sync_customers_count END,
                        sync_products_count = CASE WHEN %s = 'products' THEN %s ELSE sync_products_count END,
                        sync_orders_count = CASE WHEN %s = 'orders' THEN %s ELSE sync_orders_count END,
                        sync_line_items_count = CASE WHEN %s = 'line_items' THEN %s ELSE sync_line_items_count END,
                        sync_error = %s,
                        updated_at = NOW()
                    WHERE shop_id = %s
                    """,
                    (
                        stage, status,
                        stage, count,
                        stage, count,
                        stage, count,
                        stage, count,
                        error,
                        shop_id
                    ),
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to update sync progress: {e}")


async def mark_sync_stage_complete(shop_id: int, stage: str, count: int):
    """Mark a specific sync stage as completed."""
    from commerce_app.core.db import get_conn
    
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # Update the specific stage count and mark as completed
                if stage == 'customers':
                    await cur.execute(
                        "UPDATE shopify.shops SET sync_customers_count = %s, sync_customers_completed = TRUE WHERE shop_id = %s",
                        (count, shop_id)
                    )
                elif stage == 'products':
                    await cur.execute(
                        "UPDATE shopify.shops SET sync_products_count = %s, sync_products_completed = TRUE WHERE shop_id = %s",
                        (count, shop_id)
                    )
                elif stage == 'orders':
                    await cur.execute(
                        "UPDATE shopify.shops SET sync_orders_count = %s, sync_orders_completed = TRUE, initial_sync_order_count = %s WHERE shop_id = %s",
                        (count, count, shop_id)
                    )
                elif stage == 'line_items':
                    await cur.execute(
                        "UPDATE shopify.shops SET sync_line_items_count = %s, sync_line_items_completed = TRUE WHERE shop_id = %s",
                        (count, shop_id)
                    )
                await conn.commit()
    except Exception as e:
        print(f"Failed to mark sync stage complete: {e}")


async def mark_full_sync_complete(shop_id: int):
    """Mark the entire sync process as completed."""
    from commerce_app.core.db import get_conn
    
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE shopify.shops 
                    SET 
                        initial_sync_status = 'completed',
                        initial_sync_completed_at = NOW(),
                        sync_current_stage = 'completed',
                        sync_stage_status = 'completed'
                    WHERE shop_id = %s
                    """,
                    (shop_id,)
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to mark full sync complete: {e}")


async def mark_sync_failed(shop_id: int, error_message: str, stage: str = None):
    """Mark sync as failed in database."""
    from commerce_app.core.db import get_conn
    
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE shopify.shops 
                    SET 
                        initial_sync_status = 'failed',
                        initial_sync_error = %s,
                        sync_current_stage = %s,
                        sync_stage_status = 'failed',
                        sync_error = %s
                    WHERE shop_id = %s
                    """,
                    (error_message, stage or 'unknown', error_message, shop_id)
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to mark sync as failed: {e}")


async def register_webhooks(shop: str, access_token: str):
    """
    Register webhooks with Shopify after app installation.
    """
    webhooks_to_create = [
        {"topic": "orders/create", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "orders/updated", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "products/create", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "products/update", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "customers/create", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "customers/update", "address": f"{APP_URL}/webhooks/ingest"},
        {"topic": "app_subscriptions/update", "address": f"{APP_URL}/webhooks/ingest"},
    ]

    async with httpx.AsyncClient(timeout=20.0) as client:
        for webhook_config in webhooks_to_create:
            try:
                response = await client.post(
                    f"https://{shop}/admin/api/2025-10/webhooks.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json",
                    },
                    json={"webhook": webhook_config},
                )

                if response.status_code == 201:
                    print(f"✅ Registered webhook: {webhook_config['topic']} for {shop}")
                elif response.status_code == 422:
                    print(
                        f"⚠️  Webhook already exists: {webhook_config['topic']} for {shop}"
                    )
                else:
                    print(
                        f"❌ Failed to register webhook {webhook_config['topic']}: {response.text}"
                    )

            except Exception as e:
                print(f"❌ Error registering webhook {webhook_config['topic']}: {e}")


async def initial_data_sync(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL existing orders using Shopify Bulk Operations API (GraphQL).
    Much faster than REST pagination - no rate limits!
    Runs in background, updates progress in database.
    """
    print(f"🔄 Starting bulk initial sync for {shop}")

    from commerce_app.core.db import get_conn
    from commerce_app.core.routers.webhooks import process_order_webhook

    # Mark orders sync as in progress
    await update_sync_progress(shop_id, 'orders', 'in_progress', 0)

    # Step 1: Start bulk operation
    bulk_query = """
    {
      orders {
        edges {
          node {
            id
            name
            email
            createdAt
            updatedAt
            totalPriceSet { shopMoney { amount currencyCode } }
            subtotalPriceSet { shopMoney { amount } }
            totalTaxSet { shopMoney { amount } }
            displayFinancialStatus
            displayFulfillmentStatus
            customer { 
              id 
              email 
            }
            customerJourneySummary {
                firstVisit{
                    landingPage
                    landingPageHtml
                    referrerURL
                    source
                    sourceType
                    sourceDescription
                    utmParameters {
                        campaign
                        content
                        medium
                        source
                        term
                    }
                }
            }
            lineItems {
              edges {
                node {
                  id
                  title
                  quantity
                  originalUnitPriceSet { shopMoney { amount } }
                }
              }
            }
          }
        }
      }
    }
    """

    escaped_query = (
        bulk_query.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    )

    mutation = f'''
    mutation {{
      bulkOperationRunQuery(
        query: "{escaped_query}"
      ) {{
        bulkOperation {{
          id
          status
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    '''

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                f"https://{shop}/admin/api/2025-10/graphql.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json",
                },
                json={"query": mutation},
            )

            if response.status_code != 200:
                print(f"Failed to start bulk operation: {response.text}")
                await mark_sync_failed(shop_id, "Failed to start bulk operation", "orders")
                return 0

            data = response.json()

            if (
                "errors" in data
                or data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            ):
                print(f"GraphQL errors: {data}")
                await mark_sync_failed(shop_id, "GraphQL errors", "orders")
                return 0

            operation_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
            print(f"✅ Started bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting bulk operation: {e}")
            await mark_sync_failed(shop_id, str(e), "orders")
            return 0

        # Step 2: Poll until complete
        status_query = """
        query {
          node(id: "%s") {
            ... on BulkOperation {
              id
              status
              errorCode
              objectCount
              fileSize
              url
              partialDataUrl
            }
          }
        }
        """ % operation_id

        jsonl_url = None
        max_wait = 600
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print(f"Bulk operation timed out after {max_wait}s")
                await mark_sync_failed(shop_id, "Timeout", "orders")
                return 0

            try:
                response = await client.post(
                    f"https://{shop}/admin/api/2025-10/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json",
                    },
                    json={"query": status_query},
                )

                if response.status_code != 200:
                    await asyncio.sleep(2)
                    continue

                data = response.json()
                operation = data.get("data", {}).get("node", {})
                status = operation.get("status")

                print(
                    f"📊 Bulk operation status: {status} ({operation.get('objectCount', 0)} objects)"
                )

                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print(
                        f"✅ Bulk operation completed: {operation.get('objectCount')} orders"
                    )
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    print(
                        f"Bulk operation failed: {status} - {operation.get('errorCode')}"
                    )
                    jsonl_url = operation.get("partialDataUrl")
                    break

                await asyncio.sleep(2)

            except Exception as e:
                print(f"Error polling bulk operation: {e}")
                await asyncio.sleep(2)
                continue

        if not jsonl_url:
            print("No data URL returned from bulk operation")
            await mark_sync_failed(shop_id, "No data URL", "orders")
            return 0

        # Step 3: Download and process JSONL file
        print(f"📥 Downloading bulk data from {jsonl_url}")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download bulk data: {response.status_code}")
                await mark_sync_failed(shop_id, "Download failed", "orders")
                return 0

        except Exception as e:
            print(f"Error downloading bulk data: {e}")
            await mark_sync_failed(shop_id, str(e), "orders")
            return 0

        lines = response.text.strip().split("\n")
        total_orders = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for line in lines:
                    if not line.strip():
                        continue

                    try:
                        item = json.loads(line)
                        
                        item_id = item.get("id", "")
                        if "/Order/" not in item_id:
                            continue

                        journey = item.get("customerJourneySummary", {})
                        first_visit = journey.get("firstVisit", {}) if journey else {}
                        utm_params = first_visit.get("utmParameters", {}) if first_visit else {}
                        
                        landing_page = first_visit.get("landingPage", "") if first_visit else None
                        landing_site = None
                        
                        if landing_page and utm_params:
                            utm_parts = []
                            if utm_params.get("source"):
                                utm_parts.append(f"utm_source={utm_params['source']}")
                            if utm_params.get("medium"):
                                utm_parts.append(f"utm_medium={utm_params['medium']}")
                            if utm_params.get("campaign"):
                                utm_parts.append(f"utm_campaign={utm_params['campaign']}")
                            if utm_params.get("content"):
                                utm_parts.append(f"utm_content={utm_params['content']}")
                            if utm_params.get("term"):
                                utm_parts.append(f"utm_term={utm_params['term']}")
                            
                            if utm_parts:
                                separator = "?" if "?" not in landing_page else "&"
                                landing_site = f"{landing_page}{separator}{'&'.join(utm_parts)}"
                            else:
                                landing_site = landing_page
                        elif landing_page:
                            landing_site = landing_page
                        
                        rest_format_order = {
                            "id": item.get("id", "").split("/")[-1],
                            "name": item.get("name"),
                            "order_number": item.get("name", "").replace("#", ""),
                            "email": item.get("email"),
                            "total_price": item.get("totalPriceSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "subtotal_price": item.get("subtotalPriceSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "total_tax": item.get("totalTaxSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "currency": item.get("totalPriceSet", {})
                            .get("shopMoney", {})
                            .get("currencyCode", "USD"),
                            "financial_status": item.get("displayFinancialStatus"),
                            "fulfillment_status": item.get("displayFulfillmentStatus"),
                            "created_at": item.get("createdAt"),
                            "updated_at": item.get("updatedAt"),
                            "customer": {
                                "id": item.get("customer", {})
                                .get("id", "")
                                .split("/")[-1]
                                if item.get("customer")
                                else None
                            },
                            "line_items": item.get("lineItems", {}).get("edges", []),
                        }

                        await process_order_webhook(cur, shop_id, rest_format_order)
                        total_orders += 1

                        if total_orders % 100 == 0:
                            await conn.commit()
                            print(f"📦 Processed {total_orders} orders...")
                            await update_sync_progress(shop_id, 'orders', 'in_progress', total_orders)

                    except Exception as e:
                        print(f"Error processing order line: {e}")
                        errors += 1
                        continue

                await conn.commit()

                # Update customer total_spent based on actual orders
                print(f"📊 Calculating customer total_spent from orders...")
                await cur.execute(
                    """
                    UPDATE shopify.customers c
                    SET total_spent = COALESCE(order_totals.total, 0)
                    FROM (
                        SELECT 
                            customer_id,
                            SUM(total_price) as total
                        FROM shopify.orders
                        WHERE shop_id = %s
                          AND customer_id IS NOT NULL
                          AND financial_status IN ('paid', 'authorized', 'partially_paid')
                        GROUP BY customer_id
                    ) AS order_totals
                    WHERE c.shop_id = %s
                      AND c.customer_id = order_totals.customer_id
                    """,
                    (shop_id, shop_id)
                )
                updated_count = cur.rowcount
                await conn.commit()
                print(f"✅ Updated total_spent for {updated_count} customers based on orders")

        # Mark orders stage as complete
        await mark_sync_stage_complete(shop_id, 'orders', total_orders)
        print(f"✅ Bulk sync complete for {shop}: {total_orders} orders imported ({errors} errors)")
        
        return total_orders


async def sync_products(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL products and variants using Shopify Bulk Operations API (GraphQL).
    """
    print(f"🔄 Starting bulk product sync for {shop}")

    from commerce_app.core.db import get_conn
    from commerce_app.core.routers.webhooks import process_product_webhook

    # Mark products sync as in progress
    await update_sync_progress(shop_id, 'products', 'in_progress', 0)

    bulk_query = """
    {
      products {
        edges {
          node {
            id
            title
            handle
            vendor
            productType
            tags
            status
            createdAt
            updatedAt
            variants {
              edges {
                node {
                  id
                  title
                  price
                  sku
                  position
                  inventoryPolicy
                  compareAtPrice
                  createdAt
                  updatedAt
                  taxable
                  barcode
                  selectedOptions {
                    name
                    value
                  }
                  inventoryItem {
                    id
                    tracked
                    requiresShipping
                    measurement {
                      weight {
                        unit
                        value
                      }
                    }
                  }
                  inventoryQuantity
                }
              }
            }
          }
        }
      }
    }
    """

    escaped_query = (
        bulk_query.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    )

    mutation = f'''
    mutation {{
      bulkOperationRunQuery(query: "{escaped_query}") {{
        bulkOperation {{ id status }}
        userErrors {{ field message }}
      }}
    }}
    '''

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                f"https://{shop}/admin/api/2025-10/graphql.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json",
                },
                json={"query": mutation},
            )

            if response.status_code != 200:
                print(f"Failed to start product bulk operation: {response.text}")
                await update_sync_progress(shop_id, 'products', 'failed', 0, "Failed to start bulk operation")
                return 0

            data = response.json()

            if (
                "errors" in data
                or data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            ):
                print(f"GraphQL errors: {data}")
                await update_sync_progress(shop_id, 'products', 'failed', 0, "GraphQL errors")
                return 0

            operation_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
            print(f"✅ Started product bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting product bulk operation: {e}")
            await update_sync_progress(shop_id, 'products', 'failed', 0, str(e))
            return 0

        # Poll for completion
        status_query = """
        query {
          node(id: "%s") {
            ... on BulkOperation {
              id
              status
              errorCode
              objectCount
              url
              partialDataUrl
            }
          }
        }
        """ % operation_id

        jsonl_url = None
        max_wait = 600
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print("Product bulk operation timed out")
                await update_sync_progress(shop_id, 'products', 'failed', 0, "Timeout")
                return 0

            await asyncio.sleep(2)

            try:
                response = await client.post(
                    f"https://{shop}/admin/api/2025-10/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json",
                    },
                    json={"query": status_query},
                )

                if response.status_code != 200:
                    continue

                data = response.json()
                operation = data.get("data", {}).get("node", {})
                status = operation.get("status")

                print(
                    f"📊 Product sync status: {status} ({operation.get('objectCount', 0)} objects)"
                )

                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print("✅ Product bulk operation completed")
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    print(f"Product sync failed: {status}")
                    jsonl_url = operation.get("partialDataUrl")
                    break

            except Exception as e:
                print(f"Error polling product bulk operation: {e}")
                continue

        if not jsonl_url:
            print("No product data URL")
            await update_sync_progress(shop_id, 'products', 'failed', 0, "No data URL")
            return 0

        # Download and process
        print("📥 Downloading product data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download product data: {response.status_code}")
                await update_sync_progress(shop_id, 'products', 'failed', 0, "Download failed")
                return 0

        except Exception as e:
            print(f"Error downloading product data: {e}")
            await update_sync_progress(shop_id, 'products', 'failed', 0, str(e))
            return 0

        lines = response.text.strip().split("\n")
        products_map = {}

        for line in lines:
            if not line.strip():
                continue

            try:
                item = json.loads(line)
                item_id = item.get("id", "")

                if "/Product/" in item_id:
                    product_id = item_id.split("/")[-1]
                    products_map[product_id] = {
                        "id": product_id,
                        "title": item.get("title"),
                        "handle": item.get("handle"),
                        "vendor": item.get("vendor"),
                        "productType": item.get("productType"),
                        "tags": item.get("tags"),
                        "status": item.get("status"),
                        "createdAt": item.get("createdAt"),
                        "updatedAt": item.get("updatedAt"),
                        "variants": [],
                    }
                elif "/ProductVariant/" in item_id:
                    parent_id = item.get("__parentId", "").split("/")[-1]
                    if parent_id in products_map:
                        inventory_item = item.get("inventoryItem", {})
                        measurement = inventory_item.get("measurement", {})
                        weight_data = measurement.get("weight", {})

                        variant_data = {
                            "id": item_id.split("/")[-1],
                            "title": item.get("title"),
                            "price": item.get("price"),
                            "sku": item.get("sku"),
                            "position": item.get("position"),
                            "inventoryPolicy": item.get("inventoryPolicy"),
                            "compareAtPrice": item.get("compareAtPrice"),
                            "createdAt": item.get("createdAt"),
                            "updatedAt": item.get("updatedAt"),
                            "taxable": item.get("taxable"),
                            "barcode": item.get("barcode"),
                            "weight": weight_data.get("value"),
                            "weightUnit": weight_data.get("unit"),
                            "inventoryQuantity": item.get("inventoryQuantity"),
                        }

                        selected_options = item.get("selectedOptions", [])
                        for i, opt in enumerate(selected_options[:3], 1):
                            variant_data[f"option{i}"] = opt.get("value")

                        if inventory_item:
                            variant_data["inventoryItemId"] = (
                                inventory_item.get("id", "").split("/")[-1]
                            )
                            variant_data["inventoryManagement"] = (
                                "shopify" if inventory_item.get("tracked") else None
                            )
                            variant_data["requiresShipping"] = inventory_item.get(
                                "requiresShipping"
                            )

                        products_map[parent_id]["variants"].append(variant_data)
            except Exception as e:
                print(f"Error parsing product line: {e}")
                continue

        total_products = 0
        total_variants = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for product_data in products_map.values():
                    try:
                        await process_product_webhook(cur, shop_id, product_data)
                        total_products += 1
                        total_variants += len(product_data["variants"])

                        if total_products % 50 == 0:
                            await conn.commit()
                            print(f"📦 Processed {total_products} products, {total_variants} variants...")
                            await update_sync_progress(shop_id, 'products', 'in_progress', total_products)
                    except Exception as e:
                        print(f"Error processing product {product_data.get('id')}: {e}")
                        errors += 1
                        continue

                await conn.commit()

        # Mark products stage as complete
        await mark_sync_stage_complete(shop_id, 'products', total_products)
        print(f"✅ Product sync complete: {total_products} products, {total_variants} variants ({errors} errors)")
        
        return total_products


async def sync_product_variants(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL product variants using Shopify Bulk Operations API (GraphQL).
    """
    print(f"🔄 Starting bulk product variants sync for {shop}")

    from commerce_app.core.db import get_conn

    bulk_query = """
    {
      productVariants {
        edges {
          node {
            id
            title
            price
            sku
            position
            inventoryPolicy
            compareAtPrice
            createdAt
            updatedAt
            taxable
            barcode
            selectedOptions {
              name
              value
            }
            inventoryItem {
              id
              tracked
              requiresShipping
              measurement {
                weight {
                  unit
                  value
                }
              }
            }
            inventoryQuantity
            product {
              id
            }
          }
        }
      }
    }
    """

    escaped_query = (
        bulk_query.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    )

    mutation = f'''
    mutation {{
      bulkOperationRunQuery(query: "{escaped_query}") {{
        bulkOperation {{ id status }}
        userErrors {{ field message }}
      }}
    }}
    '''

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                f"https://{shop}/admin/api/2025-10/graphql.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json",
                },
                json={"query": mutation},
            )

            if response.status_code != 200:
                print(f"Failed to start variant bulk operation: {response.text}")
                return

            data = response.json()

            if (
                "errors" in data
                or data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            ):
                print(f"GraphQL errors: {data}")
                return

            operation_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
            print(f"✅ Started variant bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting variant bulk operation: {e}")
            return

        status_query = """
        query {
          node(id: "%s") {
            ... on BulkOperation {
              id
              status
              errorCode
              objectCount
              url
              partialDataUrl
            }
          }
        }
        """ % operation_id

        jsonl_url = None
        max_wait = 600
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print("Variant bulk operation timed out")
                return

            await asyncio.sleep(2)

            try:
                response = await client.post(
                    f"https://{shop}/admin/api/2025-10/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json",
                    },
                    json={"query": status_query},
                )

                if response.status_code != 200:
                    continue

                data = response.json()
                operation = data.get("data", {}).get("node", {})
                status = operation.get("status")

                print(f"📊 Variant sync status: {status} ({operation.get('objectCount', 0)} objects)")

                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print("✅ Variant bulk operation completed")
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    print(f"Variant sync failed: {status}")
                    jsonl_url = operation.get("partialDataUrl")
                    break

            except Exception as e:
                print(f"Error polling variant bulk operation: {e}")
                continue

        if not jsonl_url:
            print("No variant data URL")
            return

        print("📥 Downloading variant data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download variant data: {response.status_code}")
                return

        except Exception as e:
            print(f"Error downloading variant data: {e}")
            return

        lines = response.text.strip().split("\n")
        total_variants = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for line in lines:
                    if not line.strip():
                        continue

                    try:
                        variant = json.loads(line)
                        variant_id = variant.get("id", "").split("/")[-1]
                        product_id = (
                            variant.get("product", {}).get("id", "").split("/")[-1]
                        )

                        selected_options = variant.get("selectedOptions", [])
                        option1 = (
                            selected_options[0].get("value")
                            if len(selected_options) > 0
                            else None
                        )
                        option2 = (
                            selected_options[1].get("value")
                            if len(selected_options) > 1
                            else None
                        )
                        option3 = (
                            selected_options[2].get("value")
                            if len(selected_options) > 2
                            else None
                        )

                        inventory_item = variant.get("inventoryItem", {})
                        measurement = inventory_item.get("measurement", {})
                        weight_data = measurement.get("weight", {})
                        weight = weight_data.get("value")
                        weight_unit = weight_data.get("unit")

                        inventory_item_id = (
                            inventory_item.get("id", "").split("/")[-1]
                            if inventory_item
                            else None
                        )
                        inventory_management = (
                            "shopify" if inventory_item.get("tracked") else None
                        )
                        requires_shipping = inventory_item.get("requiresShipping")

                        await cur.execute(
                            """
                            INSERT INTO shopify.product_variants (
                                shop_id, variant_id, product_id, title, price, sku, 
                                position, inventory_policy, compare_at_price, 
                                option1, option2, option3, created_at, updated_at, 
                                taxable, barcode, weight, weight_unit, 
                                inventory_item_id, inventory_quantity, 
                                inventory_management, requires_shipping
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (shop_id, variant_id) 
                            DO UPDATE SET
                                title = EXCLUDED.title,
                                price = EXCLUDED.price,
                                sku = EXCLUDED.sku,
                                position = EXCLUDED.position,
                                inventory_policy = EXCLUDED.inventory_policy,
                                compare_at_price = EXCLUDED.compare_at_price,
                                option1 = EXCLUDED.option1,
                                option2 = EXCLUDED.option2,
                                option3 = EXCLUDED.option3,
                                updated_at = EXCLUDED.updated_at,
                                taxable = EXCLUDED.taxable,
                                barcode = EXCLUDED.barcode,
                                weight = EXCLUDED.weight,
                                weight_unit = EXCLUDED.weight_unit,
                                inventory_item_id = EXCLUDED.inventory_item_id,
                                inventory_quantity = EXCLUDED.inventory_quantity,
                                inventory_management = EXCLUDED.inventory_management,
                                requires_shipping = EXCLUDED.requires_shipping
                            """,
                            (
                                shop_id,
                                variant_id,
                                product_id,
                                variant.get("title"),
                                variant.get("price"),
                                variant.get("sku"),
                                variant.get("position"),
                                variant.get("inventoryPolicy"),
                                variant.get("compareAtPrice"),
                                option1,
                                option2,
                                option3,
                                variant.get("createdAt"),
                                variant.get("updatedAt"),
                                variant.get("taxable"),
                                variant.get("barcode"),
                                weight,
                                weight_unit,
                                inventory_item_id,
                                variant.get("inventoryQuantity"),
                                inventory_management,
                                requires_shipping,
                            ),
                        )

                        total_variants += 1

                        if total_variants % 100 == 0:
                            await conn.commit()
                            print(f"📦 Processed {total_variants} variants...")

                    except Exception as e:
                        print(f"Error processing variant: {e}")
                        errors += 1
                        continue

                await conn.commit()

        print(f"✅ Variant sync complete: {total_variants} variants ({errors} errors)")


async def sync_customers(shop: str, shop_id: int, access_token: str):
    """
    Extract customers using Shopify REST API with cursor-based pagination.
    """
    print(f"🔄 Starting customer extraction for {shop}")

    from commerce_app.core.db import get_conn

    # Mark customers sync as in progress
    await update_sync_progress(shop_id, 'customers', 'in_progress', 0)

    async with httpx.AsyncClient(timeout=30.0) as client:
        total_customers = 0
        page_info = None
        
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                while True:
                    try:
                        params = {"limit": 250}
                        if page_info:
                            params["page_info"] = page_info
                        
                        response = await client.get(
                            f"https://{shop}/admin/api/2025-10/customers.json",
                            headers={"X-Shopify-Access-Token": access_token},
                            params=params
                        )
                        
                        if response.status_code != 200:
                            print(f"⚠️  Customer API returned {response.status_code}: {response.text}")
                            break
                        
                        data = response.json()
                        customers = data.get("customers", [])
                        
                        if not customers:
                            print(f"✅ No more customers to fetch")
                            break
                        
                        for customer in customers:
                            try:
                                customer_id = customer.get("id")
                                
                                await cur.execute(
                                    """
                                    INSERT INTO shopify.customers (
                                        shop_id, customer_id, email, first_name, last_name,
                                        accepts_marketing, created_at, updated_at, phone,
                                        total_spent, orders_count, state, raw_json
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (shop_id, customer_id)
                                    DO UPDATE SET
                                        email = EXCLUDED.email,
                                        first_name = EXCLUDED.first_name,
                                        last_name = EXCLUDED.last_name,
                                        accepts_marketing = EXCLUDED.accepts_marketing,
                                        updated_at = EXCLUDED.updated_at,
                                        phone = EXCLUDED.phone,
                                        orders_count = EXCLUDED.orders_count,
                                        state = EXCLUDED.state,
                                        raw_json = EXCLUDED.raw_json
                                    """,
                                    (
                                        shop_id,
                                        int(customer_id),
                                        customer.get("email"),
                                        customer.get("first_name"),
                                        customer.get("last_name"),
                                        customer.get("accepts_marketing", False),
                                        customer.get("created_at"),
                                        customer.get("updated_at"),
                                        customer.get("phone"),
                                        0.0,
                                        int(customer.get("orders_count", 0)),
                                        customer.get("state", "disabled"),
                                        json.dumps(customer),
                                    ),
                                )
                                total_customers += 1
                                
                            except Exception as e:
                                print(f"Error processing customer {customer.get('id')}: {e}")
                                continue
                        
                        await conn.commit()
                        print(f"👥 Processed {total_customers} customers...")
                        
                        # Update progress
                        await update_sync_progress(shop_id, 'customers', 'in_progress', total_customers)
                        
                        link_header = response.headers.get("Link", "")
                        if 'rel="next"' in link_header:
                            import re
                            match = re.search(r'page_info=([^>&]+)', link_header)
                            if match:
                                page_info = match.group(1)
                            else:
                                break
                        else:
                            break
                        
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        print(f"Error fetching customers: {e}")
                        import traceback
                        traceback.print_exc()
                        break

    # Mark customers stage as complete
    await mark_sync_stage_complete(shop_id, 'customers', total_customers)
    print(f"✅ Customer extraction complete: {total_customers} customers imported")
    
    return total_customers


async def sync_order_line_items(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL order line items using Shopify Bulk Operations API (GraphQL).
    """
    print(f"🔄 Starting bulk order line items sync for {shop}")

    from commerce_app.core.db import get_conn

    # Mark line_items sync as in progress
    await update_sync_progress(shop_id, 'line_items', 'in_progress', 0)

    bulk_query = """
    {
      orders {
        edges {
          node {
            id
            name
            lineItems {
              edges {
                node {
                  id
                  title
                  quantity
                  variantTitle
                  name
                  sku
                  variant {
                    id
                  }
                  product {
                    id
                  }
                  originalUnitPriceSet {
                    shopMoney {
                      amount
                      currencyCode
                    }
                  }
                  discountedUnitPriceSet {
                    shopMoney {
                      amount
                      currencyCode
                    }
                  }
                  originalTotalSet {
                    shopMoney {
                      amount
                    }
                  }
                  discountedTotalSet {
                    shopMoney {
                      amount
                    }
                  }
                  taxable
                  requiresShipping
                  fulfillableQuantity
                  fulfillmentStatus
                }
              }
            }
          }
        }
      }
    }
    """

    escaped_query = (
        bulk_query.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    )

    mutation = f'''
    mutation {{
      bulkOperationRunQuery(query: "{escaped_query}") {{
        bulkOperation {{ id status }}
        userErrors {{ field message }}
      }}
    }}
    '''

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                f"https://{shop}/admin/api/2025-10/graphql.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json",
                },
                json={"query": mutation},
            )

            if response.status_code != 200:
                print(f"Failed to start line items bulk operation: {response.text}")
                await update_sync_progress(shop_id, 'line_items', 'failed', 0, "Failed to start bulk operation")
                return 0

            data = response.json()

            if (
                "errors" in data
                or data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            ):
                print(f"GraphQL errors: {data}")
                await update_sync_progress(shop_id, 'line_items', 'failed', 0, "GraphQL errors")
                return 0

            operation_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
            print(f"✅ Started line items bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting line items bulk operation: {e}")
            await update_sync_progress(shop_id, 'line_items', 'failed', 0, str(e))
            return 0

        status_query = """
        query {
          node(id: "%s") {
            ... on BulkOperation {
              id
              status
              errorCode
              objectCount
              url
              partialDataUrl
            }
          }
        }
        """ % operation_id

        jsonl_url = None
        max_wait = 600
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print("Line items bulk operation timed out")
                await update_sync_progress(shop_id, 'line_items', 'failed', 0, "Timeout")
                return 0

            await asyncio.sleep(2)

            try:
                response = await client.post(
                    f"https://{shop}/admin/api/2025-10/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json",
                    },
                    json={"query": status_query},
                )

                if response.status_code != 200:
                    continue

                data = response.json()
                operation = data.get("data", {}).get("node", {})
                status = operation.get("status")

                print(f"📊 Line items sync status: {status} ({operation.get('objectCount', 0)} objects)")

                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print("✅ Line items bulk operation completed")
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    print(f"Line items sync failed: {status}")
                    jsonl_url = operation.get("partialDataUrl")
                    break

            except Exception as e:
                print(f"Error polling line items bulk operation: {e}")
                continue

        if not jsonl_url:
            print("No line items data URL")
            await update_sync_progress(shop_id, 'line_items', 'failed', 0, "No data URL")
            return 0

        print("📥 Downloading line items data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download line items data: {response.status_code}")
                await update_sync_progress(shop_id, 'line_items', 'failed', 0, "Download failed")
                return 0

        except Exception as e:
            print(f"Error downloading line items data: {e}")
            await update_sync_progress(shop_id, 'line_items', 'failed', 0, str(e))
            return 0

        lines = response.text.strip().split("\n")
        orders_map = {}

        for line in lines:
            if not line.strip():
                continue

            try:
                item = json.loads(line)
                item_id = item.get("id", "")

                if "/Order/" in item_id:
                    order_id = item_id.split("/")[-1]
                    orders_map[order_id] = {
                        "id": order_id,
                        "name": item.get("name"),
                        "line_items": [],
                    }
                elif "/LineItem/" in item_id:
                    parent_id = item.get("__parentId", "").split("/")[-1]
                    if parent_id in orders_map:
                        orders_map[parent_id]["line_items"].append(item)

            except Exception as e:
                print(f"Error parsing line item: {e}")
                continue

        total_line_items = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for order_data in orders_map.values():
                    order_id = order_data["id"]
                    line_number = 1

                    for line_item in order_data["line_items"]:
                        try:
                            line_item_id = line_item.get("id", "").split("/")[-1]
                            variant_id = (
                                line_item.get("variant", {})
                                .get("id", "")
                                .split("/")[-1]
                                if line_item.get("variant")
                                else None
                            )
                            product_id = (
                                line_item.get("product", {})
                                .get("id", "")
                                .split("/")[-1]
                                if line_item.get("product")
                                else None
                            )

                            original_total = float(
                                line_item.get("originalTotalSet", {})
                                .get("shopMoney", {})
                                .get("amount", 0)
                            )
                            discounted_total = float(
                                line_item.get("discountedTotalSet", {})
                                .get("shopMoney", {})
                                .get("amount", 0)
                            )
                            total_discount = original_total - discounted_total

                            unit_price = (
                                line_item.get("discountedUnitPriceSet", {})
                                .get("shopMoney", {})
                                .get("amount")
                            )

                            await cur.execute(
                                """
                                INSERT INTO shopify.order_line_items (
                                    shop_id, order_id, line_number, product_id, variant_id,
                                    title, quantity,
                                    price, total_discount
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                                ON CONFLICT (shop_id, order_id, line_number)
                                DO UPDATE SET
                                    product_id = EXCLUDED.product_id,
                                    variant_id = EXCLUDED.variant_id,
                                    title = EXCLUDED.title,
                                    quantity = EXCLUDED.quantity,
                                    price = EXCLUDED.price,
                                    total_discount = EXCLUDED.total_discount
                                """,
                                (
                                    shop_id,
                                    int(order_id),
                                    line_number,
                                    int(product_id) if product_id else None,
                                    int(variant_id) if variant_id else None,
                                    line_item.get("title"),
                                    int(line_item.get("quantity", 0)),
                                    float(unit_price) if unit_price else 0.0,
                                    float(total_discount),
                                ),
                            )

                            line_number += 1
                            total_line_items += 1

                            if total_line_items % 100 == 0:
                                await conn.commit()
                                print(f"📦 Processed {total_line_items} line items...")
                                await update_sync_progress(shop_id, 'line_items', 'in_progress', total_line_items)

                        except Exception as e:
                            print(f"Error processing line item: {e}")
                            errors += 1
                            await conn.rollback()
                            continue

                await conn.commit()

    # Mark line_items stage as complete
    await mark_sync_stage_complete(shop_id, 'line_items', total_line_items)
    print(f"✅ Line items sync complete: {total_line_items} line items ({errors} errors)")
    
    return total_line_items


# ============================================================================
# SEQUENTIAL SYNC FUNCTION - Runs syncs in order to avoid foreign key violations
# ============================================================================
async def run_sequential_sync(shop: str, shop_id: int, access_token: str):
    """
    Run all sync operations SEQUENTIALLY to avoid foreign key violations.
    
    Order matters:
    1. Customers first (no dependencies)
    2. Products (no dependencies)
    3. Orders (depends on customers existing)
    4. Line items (depends on orders existing)
    """
    try:
        # 1. Customers FIRST
        print(f"🔄 [1/4] Starting customer sync for {shop}")
        customers_count = await sync_customers(shop, shop_id, access_token)
        print(f"✅ [1/4] Customer sync complete for {shop}: {customers_count} customers")
        
        # 2. Products
        print(f"🔄 [2/4] Starting product sync for {shop}")
        products_count = await sync_products(shop, shop_id, access_token)
        print(f"✅ [2/4] Product sync complete for {shop}: {products_count} products")
        
        # 3. Orders
        print(f"🔄 [3/4] Starting order sync for {shop}")
        orders_count = await initial_data_sync(shop, shop_id, access_token)
        print(f"✅ [3/4] Order sync complete for {shop}: {orders_count} orders")
        
        # 4. Line items
        print(f"🔄 [4/4] Starting line items sync for {shop}")
        line_items_count = await sync_order_line_items(shop, shop_id, access_token)
        print(f"✅ [4/4] Line items sync complete for {shop}: {line_items_count} line items")
        
        # Mark full sync as complete
        await mark_full_sync_complete(shop_id)
        
        print(f"🎉 All syncs completed successfully for {shop}")
        print(f"   📊 Summary: {customers_count} customers, {products_count} products, {orders_count} orders, {line_items_count} line items")
        
    except Exception as e:
        print(f"❌ Error during sequential sync for {shop}: {e}")
        import traceback
        traceback.print_exc()
        await mark_sync_failed(shop_id, str(e))


# ============================================================================
# NEW: Lightweight /auth/check endpoint for frontend AuthGate
# ============================================================================
@router.get("/check")
async def auth_check(shop: str):
    """
    Simple check to see if we have an access token stored for this shop.
    """
    if not is_valid_shop(shop):
        raise HTTPException(status_code=400, detail="Invalid shop parameter")

    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT access_token FROM shopify.shops WHERE shop_domain = %s",
            (shop,),
        )
        row = cur.fetchone()

    if row and row.get("access_token"):
        return {"ok": True}

    raise HTTPException(status_code=401, detail="No access token for shop")


@router.get("/start")
async def auth_start(request: Request, shop: str, host: Optional[str] = None):
    if not is_valid_shop(shop):
        raise HTTPException(status_code=400, detail="Invalid shop parameter")

    state = base64.urlsafe_b64encode(os.urandom(16)).decode().rstrip("=")

    if host:
        html = f"""
        <!doctype html><html><head><script>
        window.top.location.href = "{APP_URL}/auth/top?shop={shop}&state={state}";
        </script></head><body></body></html>"""
        resp = HTMLResponse(content=html)
        set_cookie(resp, "oauth_state", state)
        if host:
            set_cookie(resp, "shopify_host", host)
        return resp

    permission_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={SHOPIFY_API_KEY}"
        f"&scope={urlparse.quote(SCOPES)}"
        f"&redirect_uri={urlparse.quote(APP_URL + '/auth/callback')}"
        f"&state={state}"
    )
    if GRANT_PER_USER:
        permission_url += "&grant_options[]=per-user"

    resp = RedirectResponse(permission_url, status_code=302)
    set_cookie(resp, "oauth_state", state)
    if host:
        set_cookie(resp, "shopify_host", host)
    return resp


@router.get("/top")
async def top_level_bounce(request: Request, shop: str, state: str):
    if not is_valid_shop(shop):
        raise HTTPException(status_code=400, detail="Invalid shop")
    permission_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={SHOPIFY_API_KEY}"
        f"&scope={urlparse.quote(SCOPES)}"
        f"&redirect_uri={urlparse.quote(APP_URL + '/auth/callback')}"
        f"&state={state}"
    )
    if GRANT_PER_USER:
        permission_url += "&grant_options[]=per-user"
    resp = RedirectResponse(permission_url)
    set_cookie(resp, "oauth_state", state)
    return resp


@router.get("/callback")
async def auth_callback(request: Request, background_tasks: BackgroundTasks):
    qp = dict(request.query_params)
    hmac_ok = verify_hmac(SHOPIFY_API_SECRET, qp)
    if not hmac_ok:
        raise HTTPException(status_code=400, detail="HMAC verification failed")

    shop = qp.get("shop")
    code = qp.get("code")
    state = qp.get("state")
    if not (shop and code and state):
        raise HTTPException(status_code=400, detail="Missing shop/code/state")

    cookie_state = get_cookie(request, "oauth_state")
    if not cookie_state or cookie_state != state:
        raise HTTPException(status_code=400, detail="State mismatch")

    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT shop_id, access_token, updated_at 
            FROM shopify.shops 
            WHERE shop_domain = %s
            """,
            (shop,),
        )
        existing_shop = cur.fetchone()

        if existing_shop and existing_shop["updated_at"]:
            from datetime import datetime, timezone, timedelta

            if existing_shop["updated_at"] > datetime.now(
                timezone.utc
            ) - timedelta(seconds=30):
                print(
                    f"⚠️  Shop {shop} already installed recently, skipping duplicate callback"
                )
                redirect_url = f"https://{shop}/admin/apps/{SHOPIFY_API_KEY}"
                return RedirectResponse(url=redirect_url, status_code=302)

    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(token_url, json=payload)
        if r.status_code != 200:
            raise HTTPException(
                status_code=400, detail=f"Token exchange failed: {r.text}"
            )
        data = r.json()
        access_token = data["access_token"]
        scope = data.get("scope", "")

        print(f"🔍 REQUESTED SCOPES: {SCOPES}")
        print(f"🔍 GRANTED SCOPES: {scope}")
        print(f"🔍 TOKEN RESPONSE: {json.dumps(data, indent=2)}")

        shop_info_response = await client.get(
            f"https://{shop}/admin/api/2025-10/shop.json",
            headers={"X-Shopify-Access-Token": access_token},
        )

        if shop_info_response.status_code == 200:
            shop_data = shop_info_response.json()["shop"]
            shop_name = shop_data.get("name", "")
        else:
            shop_name = ""

    conn = db()
    with conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO shopify.shops (
                    shop_domain, 
                    shop_name, 
                    access_token, 
                    access_scope, 
                    installed_at, 
                    updated_at,
                    initial_sync_status,
                    sync_current_stage,
                    sync_stage_status,
                    sync_customers_count,
                    sync_products_count,
                    sync_orders_count,
                    sync_line_items_count,
                    sync_customers_completed,
                    sync_products_completed,
                    sync_orders_completed,
                    sync_line_items_completed
                )
                VALUES (%s, %s, %s, %s, now(), now(), 'pending', 'customers', 'pending', 0, 0, 0, 0, FALSE, FALSE, FALSE, FALSE)
                ON CONFLICT (shop_domain)
                DO UPDATE SET 
                    shop_name = EXCLUDED.shop_name,
                    access_token = EXCLUDED.access_token,
                    access_scope = EXCLUDED.access_scope,
                    updated_at = now(),
                    initial_sync_status = 'pending',
                    sync_current_stage = 'customers',
                    sync_stage_status = 'pending',
                    sync_customers_count = 0,
                    sync_products_count = 0,
                    sync_orders_count = 0,
                    sync_line_items_count = 0,
                    sync_customers_completed = FALSE,
                    sync_products_completed = FALSE,
                    sync_orders_completed = FALSE,
                    sync_line_items_completed = FALSE,
                    sync_error = NULL
                RETURNING shop_id;
                """,
                (shop, shop_name, access_token, scope),
            )
            shop_id = cur.fetchone()["shop_id"]
        except Exception as e:
            print(f"⚠️  Insert failed, fetching existing shop: {e}")
            conn.rollback()
            cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop,),
            )
            result = cur.fetchone()
            shop_id = result["shop_id"] if result else None
            if not shop_id:
                raise HTTPException(status_code=500, detail="Failed to save shop")

    try:
        await register_webhooks(shop, access_token)
        print(f"✅ Webhooks registered for {shop}")

        background_tasks.add_task(run_sequential_sync, shop, shop_id, access_token)
        print(f"📋 Sequential bulk sync queued for {shop} (customers→products→orders→line_items)")

    except Exception as e:
        print(f"❌ Failed setup for {shop}: {e}")

    redirect_url = f"https://{shop}/admin/apps/{SHOPIFY_API_KEY}"
    return RedirectResponse(url=redirect_url, status_code=302)


@router.get("/sync-status/{shop_domain}")
async def sync_status(shop_domain: str):
    """
    Check initial sync progress with detailed stage information.
    Returns current stage, counts for each stage, and completion status.
    """
    from commerce_app.core.db import get_conn

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT 
                    initial_sync_status,
                    initial_sync_completed_at,
                    initial_sync_error,
                    sync_current_stage,
                    sync_stage_status,
                    sync_customers_count,
                    sync_products_count,
                    sync_orders_count,
                    sync_line_items_count,
                    sync_customers_completed,
                    sync_products_completed,
                    sync_orders_completed,
                    sync_line_items_completed,
                    sync_error,
                    initial_sync_order_count
                   FROM shopify.shops 
                   WHERE shop_domain = %s""",
                (shop_domain,),
            )
            row = await cur.fetchone()

            if not row:
                return {"status": "not_found"}

            # Calculate overall progress percentage
            stages_completed = sum([
                1 if row[9] else 0,   # sync_customers_completed
                1 if row[10] else 0,  # sync_products_completed
                1 if row[11] else 0,  # sync_orders_completed
                1 if row[12] else 0,  # sync_line_items_completed
            ])
            
            # If currently syncing, add partial progress for current stage
            current_stage = row[3]
            stage_status = row[4]
            
            if stage_status == 'in_progress':
                # Add partial progress (0.5 for in-progress stage)
                progress_percent = (stages_completed + 0.5) / 4 * 100
            else:
                progress_percent = stages_completed / 4 * 100

            return {
                "status": row[0],  # initial_sync_status
                "completed_at": row[1].isoformat() if row[1] else None,
                "error": row[2] or row[13],  # initial_sync_error or sync_error
                
                # Detailed stage information
                "current_stage": current_stage,
                "stage_status": stage_status,
                
                # Counts for each stage
                "customers_synced": row[5] or 0,
                "products_synced": row[6] or 0,
                "orders_synced": row[7] or row[14] or 0,  # Also check initial_sync_order_count
                "line_items_synced": row[8] or 0,
                
                # Completion flags
                "customers_completed": row[9] or False,
                "products_completed": row[10] or False,
                "orders_completed": row[11] or False,
                "line_items_completed": row[12] or False,
                
                # Overall progress
                "progress_percent": progress_percent,
                "stages_completed": stages_completed,
                "total_stages": 4,
            }


# ============================================================================
# SYNC ENDPOINTS: Manually trigger syncs
# ============================================================================
@router.post("/sync-customers/{shop_domain}")
async def trigger_customer_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """Manually trigger a customer sync for a shop."""
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT shop_id, access_token FROM shopify.shops WHERE shop_domain = %s",
            (shop_domain,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Shop not found")

        shop_id = row["shop_id"]
        access_token = row["access_token"]

    background_tasks.add_task(sync_customers, shop_domain, shop_id, access_token)

    return {
        "status": "started",
        "message": f"Customer sync started for {shop_domain}",
    }


@router.post("/sync-products/{shop_domain}")
async def trigger_product_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """Manually trigger a product sync for a shop."""
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT shop_id, access_token FROM shopify.shops WHERE shop_domain = %s",
            (shop_domain,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Shop not found")

        shop_id = row["shop_id"]
        access_token = row["access_token"]

    background_tasks.add_task(sync_products, shop_domain, shop_id, access_token)

    return {
        "status": "started",
        "message": f"Product sync started for {shop_domain}",
    }


@router.post("/sync-variants/{shop_domain}")
async def trigger_variant_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """Manually trigger a product variants sync for a shop."""
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT shop_id, access_token FROM shopify.shops WHERE shop_domain = %s",
            (shop_domain,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Shop not found")

        shop_id = row["shop_id"]
        access_token = row["access_token"]

    background_tasks.add_task(sync_product_variants, shop_domain, shop_id, access_token)

    return {
        "status": "started",
        "message": f"Variant sync started for {shop_domain}",
    }


@router.post("/sync-line-items/{shop_domain}")
async def trigger_line_items_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """Manually trigger an order line items sync for a shop."""
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            "SELECT shop_id, access_token FROM shopify.shops WHERE shop_domain = %s",
            (shop_domain,),
        )
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Shop not found")

        shop_id = row["shop_id"]
        access_token = row["access_token"]

    background_tasks.add_task(sync_order_line_items, shop_domain, shop_id, access_token)

    return {
        "status": "started",
        "message": f"Line items sync started for {shop_domain}",
    }