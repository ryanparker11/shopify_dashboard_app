import base64, hashlib, hmac, time, urllib.parse as urlparse
from typing import Dict, Optional
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import httpx, os
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
SCOPES = os.environ.get("SCOPES", "read_products,read_orders")  # Added read_orders
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
                    logger.info(f"✅ Registered webhook: {webhook_config['topic']} for {shop}")
                elif response.status_code == 422:
                    logger.warning(f"⚠️  Webhook already exists: {webhook_config['topic']} for {shop}")
                else:
                    logger.error(f"❌ Failed to register webhook {webhook_config['topic']}: {response.text}")
                    
            except Exception as e:
                logger.error(f"❌ Error registering webhook {webhook_config['topic']}: {e}")


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
async def auth_callback(request: Request):
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

    # Upsert shop record (FIXED: now includes shop_name)
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shopify.shops (shop_domain, shop_name, access_token, access_scope, installed_at, updated_at)
            VALUES (%s, %s, %s, %s, now(), now())
            ON CONFLICT (shop_domain)
            DO UPDATE SET shop_name = EXCLUDED.shop_name,
                          access_token = EXCLUDED.access_token,
                          access_scope = EXCLUDED.access_scope,
                          updated_at = now()
            RETURNING id;
            """,
            (shop, shop_name, access_token, scope),
        )
        shop_id = cur.fetchone()["id"]

    # Register webhooks
    try:
        await register_webhooks(shop, access_token)
        logger.info(f"✅ Webhooks registered for {shop}")
    except Exception as e:
        logger.error(f"❌ Failed to register webhooks for {shop}: {e}")
        # Don't fail auth flow

    # Redirect to app
    host = get_cookie(request, "shopify_host")
    if not host:
        host = base64.b64encode(f"{shop}/admin".encode()).decode()

    return RedirectResponse(url=f"{APP_URL}/app?shop={shop}&host={urlparse.quote(host)}")