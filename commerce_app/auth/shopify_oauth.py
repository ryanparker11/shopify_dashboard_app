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
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://app.lodestaranalytics.io")  # ADD THIS LINE
SCOPES = os.environ.get("SCOPES", "read_products,read_orders")
GRANT_PER_USER = os.environ.get("GRANT_OPTIONS_PER_USER", "false").lower() == "true"

def db():
    return psycopg2.connect(
        os.environ["DATABASE_URL"], cursor_factory=RealDictCursor
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
                    f"https://{shop}/admin/api/2024-10/webhooks.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    },
                    json={"webhook": webhook_config}
                )
                
                if response.status_code == 201:
                    print(f"✅ Registered webhook: {webhook_config['topic']} for {shop}")
                elif response.status_code == 422:
                    print(f"⚠️  Webhook already exists: {webhook_config['topic']} for {shop}")
                else:
                    print(f"❌ Failed to register webhook {webhook_config['topic']}: {response.text}")
                    
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
                    (shop_id,)
                )
                await conn.commit()
    except Exception as e:
        print(f"Failed to update sync status: {e}")
    
    # Step 1: Start bulk operation
    # Build the GraphQL query separately to avoid escaping issues
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
            financialStatus
            fulfillmentStatus
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
    escaped_query = bulk_query.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ')
    
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
                f"https://{shop}/admin/api/2024-10/graphql.json",
                headers={
                    "X-Shopify-Access-Token": access_token,
                    "Content-Type": "application/json"
                },
                json={"query": mutation}
            )
            
            if response.status_code != 200:
                print(f"Failed to start bulk operation: {response.text}")
                await mark_sync_failed(shop_id, "Failed to start bulk operation")
                return
            
            data = response.json()
            
            if "errors" in data or data.get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors"):
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
                    f"https://{shop}/admin/api/2024-10/graphql.json",
                    headers={
                        "X-Shopify-Access-Token": access_token,
                        "Content-Type": "application/json"
                    },
                    json={"query": status_query}
                )
                
                if response.status_code != 200:
                    await asyncio.sleep(2)
                    continue
                
                data = response.json()
                operation = data.get("data", {}).get("node", {})
                status = operation.get("status")
                
                print(f"📊 Bulk operation status: {status} ({operation.get('objectCount', 0)} objects)")
                
                if status == "COMPLETED":
                    jsonl_url = operation.get("url")
                    print(f"✅ Bulk operation completed: {operation.get('objectCount')} orders")
                    break
                elif status in ["FAILED", "CANCELED", "EXPIRED"]:
                    print(f"Bulk operation failed: {status} - {operation.get('errorCode')}")
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
        lines = response.text.strip().split('\n')
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
                            "id": order.get("id", "").split("/")[-1],  # Extract numeric ID from gid://
                            "name": order.get("name"),
                            "order_number": order.get("name", "").replace("#", ""),
                            "email": order.get("email"),
                            "total_price": order.get("totalPriceSet", {}).get("shopMoney", {}).get("amount", "0"),
                            "subtotal_price": order.get("subtotalPriceSet", {}).get("shopMoney", {}).get("amount", "0"),
                            "total_tax": order.get("totalTaxSet", {}).get("shopMoney", {}).get("amount", "0"),
                            "currency": order.get("totalPriceSet", {}).get("shopMoney", {}).get("currencyCode", "USD"),
                            "financial_status": order.get("financialStatus"),
                            "fulfillment_status": order.get("fulfillmentStatus"),
                            "created_at": order.get("createdAt"),
                            "updated_at": order.get("updatedAt"),
                            "customer": {
                                "id": order.get("customer", {}).get("id", "").split("/")[-1] if order.get("customer") else None
                            },
                            "line_items": order.get("lineItems", {}).get("edges", [])
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
                                (total_orders, shop_id)
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
                    (total_orders, shop_id)
                )
                await conn.commit()
        
        print(f"✅ Bulk sync complete for {shop}: {total_orders} orders imported ({errors} errors)")


#async def mark_sync_failed(shop_id: int, error_message: str):
#    """Mark initial sync as failed in database."""
#    try:
#        from commerce_app.core.db import get_conn
#        async with get_conn() as conn:
#            async with conn.cursor() as cur:
#                await cur.execute(
#                    """UPDATE shopify.shops 
#                       SET initial_sync_status = 'failed',
#                           initial_sync_error = %s
#                       WHERE shop_id = %s""",
#                    (error_message, shop_id)
#                )
#                await conn.commit()
#    except Exception as e:
#        print(f"Failed to mark sync as failed: {e}")


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
        if host: set_cookie(resp, "shopify_host", host)
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
    if host: set_cookie(resp, "shopify_host", host)
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
            raise HTTPException(status_code=400, detail=f"Token exchange failed: {r.text}")
        data = r.json()
        access_token = data["access_token"]
        scope = data.get("scope", "")
        
        # Fetch shop details
        shop_info_response = await client.get(
            f"https://{shop}/admin/api/2024-10/shop.json",
            headers={"X-Shopify-Access-Token": access_token}
        )
        
        if shop_info_response.status_code == 200:
            shop_data = shop_info_response.json()["shop"]
            shop_name = shop_data.get("name", "")
        else:
            shop_name = ""

    # Upsert shop record
    conn = db()
    with conn, conn.cursor() as cur:
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

    # Register webhooks and queue initial sync
    try:
        await register_webhooks(shop, access_token)
        print(f"✅ Webhooks registered for {shop}")
        
        # Run initial bulk sync in background (don't wait for it)
        background_tasks.add_task(initial_data_sync, shop, shop_id, access_token)
        print(f"📋 Bulk sync queued for {shop}")
        
    except Exception as e:
        print(f"❌ Failed setup for {shop}: {e}")
        # Don't fail auth flow - merchant is still installed

    # Redirect to app immediately (don't wait for sync)
    host = get_cookie(request, "shopify_host")
    if not host:
        host = base64.b64encode(f"{shop}/admin".encode()).decode()

    return RedirectResponse(url=f"{FRONTEND_URL}/app?shop={shop}&host={urlparse.quote(host)}")


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
                (shop_domain,)
            )
            row = await cur.fetchone()
            
            if not row:
                return {"status": "not_found"}
            
            return {
                "status": row[0],
                "orders_synced": row[1] or 0,
                "completed_at": row[2].isoformat() if row[2] else None,
                "error": row[3]
            }