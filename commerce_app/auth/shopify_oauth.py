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

    # Import here to avoid circular imports
    from commerce_app.core.db import get_conn
    from commerce_app.core.routers.webhooks import process_order_webhook

    # Mark sync as in progress
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE shopify.shops SET initial_sync_status = 'in_progress' WHERE shop_id = %s",
                    (shop_id,),
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to update sync status: {e}")

    # Step 1: Start bulk operation
    # FIXED: Updated to use displayFinancialStatus and displayFulfillmentStatus
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

    # Escape the query for GraphQL mutation
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
        # Start bulk operation
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
                await mark_sync_failed(shop_id, "Failed to start bulk operation")
                return

            data = response.json()

            if (
                "errors" in data
                or data.get("data", {})
                .get("bulkOperationRunQuery", {})
                .get("userErrors")
            ):
                print(f"GraphQL errors: {data}")
                await mark_sync_failed(shop_id, "GraphQL errors")
                return

            operation_id = data["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
            print(f"✅ Started bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting bulk operation: {e}")
            await mark_sync_failed(shop_id, str(e))
            return

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
        max_wait = 600  # 10 minutes max
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print(f"Bulk operation timed out after {max_wait}s")
                await mark_sync_failed(shop_id, "Timeout")
                return

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
                    jsonl_url = operation.get("partialDataUrl")  # Try partial data
                    break

                await asyncio.sleep(2)  # Check every 2 seconds

            except Exception as e:
                print(f"Error polling bulk operation: {e}")
                await asyncio.sleep(2)
                continue

        if not jsonl_url:
            print("No data URL returned from bulk operation")
            await mark_sync_failed(shop_id, "No data URL")
            return

        # Step 3: Download and process JSONL file
        print(f"📥 Downloading bulk data from {jsonl_url}")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download bulk data: {response.status_code}")
                await mark_sync_failed(shop_id, "Download failed")
                return

        except Exception as e:
            print(f"Error downloading bulk data: {e}")
            await mark_sync_failed(shop_id, str(e))
            return

        # Process JSONL (newline-delimited JSON)
        lines = response.text.strip().split("\n")
        total_orders = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for line in lines:
                    if not line.strip():
                        continue

                    try:
                        order = json.loads(line)

                        # Convert GraphQL format to REST format for process_order_webhook
                        rest_format_order = {
                            "id": order.get("id", "").split("/")[
                                -1
                            ],  # Extract numeric ID from gid://
                            "name": order.get("name"),
                            "order_number": order.get("name", "").replace("#", ""),
                            "email": order.get("email"),
                            "total_price": order.get("totalPriceSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "subtotal_price": order.get("subtotalPriceSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "total_tax": order.get("totalTaxSet", {})
                            .get("shopMoney", {})
                            .get("amount", "0"),
                            "currency": order.get("totalPriceSet", {})
                            .get("shopMoney", {})
                            .get("currencyCode", "USD"),
                            "financial_status": order.get("displayFinancialStatus"),
                            "fulfillment_status": order.get(
                                "displayFulfillmentStatus"
                            ),
                            "created_at": order.get("createdAt"),
                            "updated_at": order.get("updatedAt"),
                            "customer": {
                                "id": order.get("customer", {})
                                .get("id", "")
                                .split("/")[-1]
                                if order.get("customer")
                                else None
                            },
                            "line_items": order.get("lineItems", {}).get("edges", []),
                        }

                        await process_order_webhook(cur, shop_id, rest_format_order)
                        total_orders += 1

                        # Commit in batches of 100 for performance and update progress
                        if total_orders % 100 == 0:
                            await conn.commit()
                            print(f"📦 Processed {total_orders} orders...")

                            # Update progress in database
                            await cur.execute(
                                "UPDATE shopify.shops SET initial_sync_order_count = %s WHERE shop_id = %s",
                                (total_orders, shop_id),
                            )
                            await conn.commit()

                    except Exception as e:
                        print(f"Error processing order line: {e}")
                        errors += 1
                        continue

                # Final commit
                await conn.commit()

                # Mark sync as complete
                await cur.execute(
                    """UPDATE shopify.shops 
                       SET initial_sync_status = 'completed',
                           initial_sync_completed_at = NOW(),
                           initial_sync_order_count = %s
                       WHERE shop_id = %s""",
                    (total_orders, shop_id),
                )
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

        print(
            f"✅ Bulk sync complete for {shop}: {total_orders} orders imported ({errors} errors)"
        )


# ============================================================================
# NEW FUNCTION: Product Sync using Bulk Operations API
# ============================================================================
async def sync_products(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL products and variants using Shopify Bulk Operations API (GraphQL).
    Much faster than REST pagination - no rate limits!
    """
    print(f"🔄 Starting bulk product sync for {shop}")

    from commerce_app.core.db import get_conn
    from commerce_app.core.routers.webhooks import process_product_webhook

    # FIXED: Removed weight/weightUnit from variant, added to inventoryItem
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

    # Escape for GraphQL mutation
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
        # Start bulk operation
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
            print(f"✅ Started product bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting product bulk operation: {e}")
            return

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
        max_wait = 600  # 10 minutes
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print("Product bulk operation timed out")
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
            return

        # Download and process
        print("📥 Downloading product data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download product data: {response.status_code}")
                return

        except Exception as e:
            print(f"Error downloading product data: {e}")
            return

        lines = response.text.strip().split("\n")
        products_map = {}

        # Parse JSONL - group products with their variants
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
                        # FIXED: Get weight from inventoryItem.measurement.weight
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

                        # Parse selectedOptions into option1, option2, option3
                        selected_options = item.get("selectedOptions", [])
                        for i, opt in enumerate(selected_options[:3], 1):
                            variant_data[f"option{i}"] = opt.get("value")

                        # Parse inventoryItem data
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

        # Save to database
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
                            print(
                                f"📦 Processed {total_products} products, {total_variants} variants..."
                            )
                    except Exception as e:
                        print(
                            f"Error processing product {product_data.get('id')}: {e}"
                        )
                        errors += 1
                        continue

                await conn.commit()

        print(
            f"✅ Product sync complete: {total_products} products, {total_variants} variants ({errors} errors)"
        )


# ============================================================================
# NEW FUNCTION: Product Variants Only Sync
# ============================================================================
async def sync_product_variants(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL product variants using Shopify Bulk Operations API (GraphQL).
    Useful for updating variant-specific data like inventory and pricing.
    """
    print(f"🔄 Starting bulk product variants sync for {shop}")

    from commerce_app.core.db import get_conn

    # FIXED: Removed weight/weightUnit from variant, added to inventoryItem
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
        # Start bulk operation
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

                print(
                    f"📊 Variant sync status: {status} ({operation.get('objectCount', 0)} objects)"
                )

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

        # Download and process
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

                        # Parse selectedOptions
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

                        # FIXED: Parse weight from inventoryItem.measurement.weight
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

                        # Update variant in database
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

        print(
            f"✅ Variant sync complete: {total_variants} variants ({errors} errors)"
        )

# ============================================================================
# NEW FUNCTION: Customer Sync using Bulk Operations API
# ============================================================================
async def sync_customers(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL customers using Shopify Bulk Operations API (GraphQL).
    Much faster than REST pagination - no rate limits!
    """
    print(f"🔄 Starting bulk customer sync for {shop}")

    from commerce_app.core.db import get_conn

    bulk_query = """
    {
      customers {
        edges {
          node {
            id
            email
            firstName
            lastName
            phone
            emailMarketingConsent {
              marketingState
            }
            createdAt
            updatedAt
            numberOfOrders
            state
          }
        }
      }
    }
    """

    # Escape for GraphQL mutation
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
        # Start bulk operation
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
                print(f"Failed to start customer bulk operation: {response.text}")
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
            print(f"✅ Started customer bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting customer bulk operation: {e}")
            return

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
        max_wait = 600  # 10 minutes
        start_time = asyncio.get_event_loop().time()

        while True:
            if asyncio.get_event_loop().time() - start_time > max_wait:
                print("Customer bulk operation timed out")
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

                print(
                    f"📊 Customer sync status: {status} ({operation.get('objectCount', 0)} objects)"
                )

                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print("✅ Customer bulk operation completed")
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    error_code = operation.get("errorCode")
                    print(f"❌ Customer sync failed: {status} - Error code: {error_code}")
                    print(f"❌ Full operation data: {json.dumps(operation, indent=2)}")
                    jsonl_url = operation.get("partialDataUrl")
                    break

            except Exception as e:
                print(f"Error polling customer bulk operation: {e}")
                continue

        if not jsonl_url:
            print("No customer data URL")
            return

        # Download and process
        print("📥 Downloading customer data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(f"Failed to download customer data: {response.status_code}")
                return

        except Exception as e:
            print(f"Error downloading customer data: {e}")
            return

        lines = response.text.strip().split("\n")
        total_customers = 0
        errors = 0

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                for line in lines:
                    if not line.strip():
                        continue

                    try:
                        customer = json.loads(line)
                        customer_id = customer.get("id", "").split("/")[-1]
                        
                        # Parse email marketing consent
                        marketing_consent = customer.get("emailMarketingConsent", {})
                        accepts_marketing = marketing_consent.get("marketingState") == "SUBSCRIBED"
                        
                        # Get orders count
                        orders_count = int(customer.get("numberOfOrders", 0))

                        # Insert/update customer (total_spent will be calculated from orders later)
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
                                customer.get("firstName"),
                                customer.get("lastName"),
                                accepts_marketing,
                                customer.get("createdAt"),
                                customer.get("updatedAt"),
                                customer.get("phone"),
                                0.0,  # total_spent will be calculated from orders
                                orders_count,
                                customer.get("state"),
                                json.dumps(customer),
                            ),
                        )

                        total_customers += 1

                        # Commit in batches of 100 for performance
                        if total_customers % 100 == 0:
                            await conn.commit()
                            print(f"👥 Processed {total_customers} customers...")

                    except Exception as e:
                        print(f"Error processing customer line: {e}")
                        errors += 1
                        await conn.rollback()
                        continue

                # Final commit
                await conn.commit()

        print(
            f"✅ Customer sync complete: {total_customers} customers imported ({errors} errors)"
        )


# ============================================================================
# NEW FUNCTION: Order Line Items Sync
# ============================================================================
async def sync_order_line_items(shop: str, shop_id: int, access_token: str):
    """
    Fetch ALL order line items using Shopify Bulk Operations API (GraphQL).
    Useful for detailed order analysis and profitability calculations.
    """
    print(f"🔄 Starting bulk order line items sync for {shop}")

    from commerce_app.core.db import get_conn

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
        # Start bulk operation
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
            print(f"✅ Started line items bulk operation: {operation_id}")

        except Exception as e:
            print(f"Error starting line items bulk operation: {e}")
            return

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
                print("Line items bulk operation timed out")
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

                print(
                    f"📊 Line items sync status: {status} ({operation.get('objectCount', 0)} objects)"
                )

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
            return

        # Download and process
        print("📥 Downloading line items data...")
        try:
            response = await client.get(jsonl_url, timeout=120.0)

            if response.status_code != 200:
                print(
                    f"Failed to download line items data: {response.status_code}"
                )
                return

        except Exception as e:
            print(f"Error downloading line items data: {e}")
            return

        lines = response.text.strip().split("\n")
        orders_map = {}

        # Parse JSONL - group line items with their orders
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

        # Save to database
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

                            # Parse pricing - calculate discount
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

                            # Use discounted unit price as the "price"
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
                                    line_number,  # Use counter instead of line_item_id
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

                        except Exception as e:
                            print(f"Error processing line item: {e}")
                            errors += 1
                            await conn.rollback()  # Rollback failed transaction
                            continue

                await conn.commit()

        print(
            f"✅ Line items sync complete: {total_line_items} line items ({errors} errors)"
        )


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
        # 1. Customers FIRST - orders have foreign keys to customers
        print(f"🔄 [1/4] Starting customer sync for {shop}")
        await sync_customers(shop, shop_id, access_token)
        print(f"✅ [1/4] Customer sync complete for {shop}")
        
        # 2. Products - independent of customers
        print(f"🔄 [2/4] Starting product sync for {shop}")
        await sync_products(shop, shop_id, access_token)
        print(f"✅ [2/4] Product sync complete for {shop}")
        
        # 3. Orders - NOW customers exist, can reference them safely
        print(f"🔄 [3/4] Starting order sync for {shop}")
        await initial_data_sync(shop, shop_id, access_token)
        print(f"✅ [3/4] Order sync complete for {shop}")
        
        # 4. Line items - depends on orders existing
        print(f"🔄 [4/4] Starting line items sync for {shop}")
        await sync_order_line_items(shop, shop_id, access_token)
        print(f"✅ [4/4] Line items sync complete for {shop}")
        
        print(f"🎉 All syncs completed successfully for {shop}")
        
    except Exception as e:
        print(f"❌ Error during sequential sync for {shop}: {e}")
        import traceback
        traceback.print_exc()


# ============================================================================
# Helper function
# ============================================================================
async def mark_sync_failed(shop_id: int, error_message: str):
    """Mark initial sync as failed in database."""
    try:
        from commerce_app.core.db import get_conn

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE shopify.shops 
                       SET initial_sync_status = 'failed',
                           initial_sync_error = %s
                       WHERE shop_id = %s""",
                    (error_message, shop_id),
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to mark sync as failed: {e}")


# ============================================================================
# NEW: Lightweight /auth/check endpoint for frontend AuthGate
# ============================================================================
@router.get("/check")
async def auth_check(shop: str):
    """
    Simple check to see if we have an access token stored for this shop.

    Used by the frontend AuthGate to decide whether to redirect the merchant
    to /auth/start (OAuth) or go straight into the embedded app.

    Returns:
      200 + {"ok": True} if the shop exists and has an access token
      401 if no access token is stored
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

    # No token yet → frontend should send merchant through /auth/start
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
    # Parse query
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

    # Check if we already have this shop installed recently
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

        # If shop was updated in last 30 seconds, skip token exchange (already done)
        if existing_shop and existing_shop["updated_at"]:
            from datetime import datetime, timezone, timedelta

            if existing_shop["updated_at"] > datetime.now(
                timezone.utc
            ) - timedelta(seconds=30):
                print(
                    f"⚠️  Shop {shop} already installed recently, skipping duplicate callback"
                )
                # Skip to redirect
                redirect_url = f"https://{shop}/admin/apps/{SHOPIFY_API_KEY}"
                return RedirectResponse(url=redirect_url, status_code=302)

    # Exchange code -> token (ONLY ONCE!)
    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        # Get access token
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

        # Fetch shop details
        shop_info_response = await client.get(
            f"https://{shop}/admin/api/2025-10/shop.json",
            headers={"X-Shopify-Access-Token": access_token},
        )

        if shop_info_response.status_code == 200:
            shop_data = shop_info_response.json()["shop"]
            shop_name = shop_data.get("name", "")
        else:
            shop_name = ""

    # Upsert shop record
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
                    initial_sync_status
                )
                VALUES (%s, %s, %s, %s, now(), now(), 'pending')
                ON CONFLICT (shop_domain)
                DO UPDATE SET 
                    shop_name = EXCLUDED.shop_name,
                    access_token = EXCLUDED.access_token,
                    access_scope = EXCLUDED.access_scope,
                    updated_at = now(),
                    initial_sync_status = 'pending'
                RETURNING shop_id;
                """,
                (shop, shop_name, access_token, scope),
            )
            shop_id = cur.fetchone()["shop_id"]
        except Exception as e:
            # If insert fails, try to get existing shop_id
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

    # Register webhooks and queue SEQUENTIAL sync
    try:
        await register_webhooks(shop, access_token)
        print(f"✅ Webhooks registered for {shop}")

        # Run SEQUENTIAL sync in background (all syncs will run in order)
        background_tasks.add_task(run_sequential_sync, shop, shop_id, access_token)
        print(f"📋 Sequential bulk sync queued for {shop} (customers→products→orders→line_items)")

    except Exception as e:
        print(f"❌ Failed setup for {shop}: {e}")
        # Don't fail auth flow - merchant is still installed

    # Redirect to Shopify admin apps page
    redirect_url = f"https://{shop}/admin/apps/{SHOPIFY_API_KEY}"
    return RedirectResponse(url=redirect_url, status_code=302)


@router.get("/sync-status/{shop_domain}")
async def sync_status(shop_domain: str):
    """
    Check initial sync progress.
    Useful for displaying sync status in your app UI.
    """
    from commerce_app.core.db import get_conn

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """SELECT 
                    initial_sync_status, 
                    initial_sync_order_count, 
                    initial_sync_completed_at,
                    initial_sync_error
                   FROM shopify.shops 
                   WHERE shop_domain = %s""",
                (shop_domain,),
            )
            row = await cur.fetchone()

            if not row:
                return {"status": "not_found"}

            return {
                "status": row[0],
                "orders_synced": row[1] or 0,
                "completed_at": row[2].isoformat() if row[2] else None,
                "error": row[3],
            }


# ============================================================================
# SYNC ENDPOINTS: Manually trigger syncs
# ============================================================================
@router.post("/sync-customers/{shop_domain}")
async def trigger_customer_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """
    Manually trigger a customer sync for a shop.
    Useful for backfilling missing customers or re-syncing.
    """
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

    # Run sync in background
    background_tasks.add_task(
        sync_customers, shop_domain, shop_id, access_token
    )

    return {
        "status": "started",
        "message": f"Customer sync started for {shop_domain}",
    }


@router.post("/sync-products/{shop_domain}")
async def trigger_product_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """
    Manually trigger a product sync for a shop.
    Useful for backfilling missing products or re-syncing.
    """
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

    # Run sync in background
    background_tasks.add_task(
        sync_products, shop_domain, shop_id, access_token
    )

    return {
        "status": "started",
        "message": f"Product sync started for {shop_domain}",
    }


@router.post("/sync-variants/{shop_domain}")
async def trigger_variant_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """
    Manually trigger a product variants sync for a shop.
    Useful for updating variant-specific data like inventory and pricing.
    """
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

    # Run sync in background
    background_tasks.add_task(
        sync_product_variants, shop_domain, shop_id, access_token
    )

    return {
        "status": "started",
        "message": f"Variant sync started for {shop_domain}",
    }


@router.post("/sync-line-items/{shop_domain}")
async def trigger_line_items_sync(
    shop_domain: str, background_tasks: BackgroundTasks
):
    """
    Manually trigger an order line items sync for a shop.
    Useful for detailed order analysis and profitability calculations.
    """
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

    # Run sync in background
    background_tasks.add_task(
        sync_order_line_items, shop_domain, shop_id, access_token
    )

    return {
        "status": "started",
        "message": f"Line items sync started for {shop_domain}",
    }