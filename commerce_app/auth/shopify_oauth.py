import base64, hashlib, hmac, time, urllib.parse as urlparse
from typing import Dict, Optional
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import httpx, os
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
load_dotenv()

router = APIRouter(prefix="/auth", tags=["shopify-auth"])

SHOPIFY_API_KEY = os.environ["SHOPIFY_API_KEY"]
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]
APP_URL = os.environ["APP_URL"].rstrip("/")
SCOPES = os.environ.get("SCOPES", "read_products")
GRANT_PER_USER = os.environ.get("GRANT_OPTIONS_PER_USER", "false").lower() == "true"

def db():
    # replace with your pool/SQLAlchemy; this is simple and synchronous for clarity
    return psycopg2.connect(
        os.environ["DATABASE_URL"], cursor_factory=RealDictCursor
    )

def is_valid_shop(shop: str) -> bool:
    # Basic guard: "<name>.myshopify.com"
    return shop.endswith(".myshopify.com") and shop.count(".") >= 2 and "/" not in shop

def sign_hmac(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

def verify_hmac(secret: str, query: Dict[str, str]) -> bool:
    # Per Shopify: exclude "hmac" and "signature"; sort keys; build "k=v&..."
    q = {k: v for k, v in query.items() if k not in ("hmac", "signature")}
    pairs = [f"{k}={v}" for k, v in sorted(q.items(), key=lambda kv: kv[0])]
    msg = "&".join(pairs)
    computed = sign_hmac(secret, msg)
    provided = query.get("hmac", "")
    # timing-safe compare (hex lowercase)
    return hmac.compare_digest(computed, provided)

def set_cookie(response: Response, name: str, value: str, max_age: int = 300):
    response.set_cookie(
        name, value, max_age=max_age, httponly=True, secure=True, samesite="none"
    )

def get_cookie(request: Request, name: str) -> Optional[str]:
    return request.cookies.get(name)

@router.get("/start")
async def auth_start(request: Request, shop: str, host: Optional[str] = None):
    # 1) Validate shop
    if not is_valid_shop(shop):
        raise HTTPException(status_code=400, detail="Invalid shop parameter")

    # 2) CSRF state/nonce
    state = base64.urlsafe_b64encode(os.urandom(16)).decode().rstrip("=")

    # 3) Are we in an embedded iframe? If so, bounce to top-level
    # Shopify sends ?host=... inside Admin; if present, do a top-level redirect page.
    if host:
        html = f"""
        <!doctype html><html><head><script>
        window.top.location.href = "{APP_URL}/auth/top?shop={shop}&state={state}";
        </script></head><body></body></html>"""
        resp = HTMLResponse(content=html)
        set_cookie(resp, "oauth_state", state)
        if host: set_cookie(resp, "shopify_host", host)
        return resp

    # 4) Direct to Shopify permission screen
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
    # This page must NOT be in iframe; it immediately redirects to /auth/start without host
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
    # Set cookie again so state is available to callback even if third-party cookie rules bite
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

    # Exchange code -> token
    token_url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(token_url, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=400, detail=f"Token exchange failed: {r.text}")
        data = r.json()
        access_token = data["access_token"]
        scope = data.get("scope", "")

    # Upsert shop record
    conn = db()
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shops (shop_domain, access_token, scope, installed_at, updated_at)
            VALUES (%s, %s, %s, now(), now())
            ON CONFLICT (shop_domain)
            DO UPDATE SET access_token = EXCLUDED.access_token,
                          scope = EXCLUDED.scope,
                          updated_at = now();
            """,
            (shop, access_token, scope),
        )

    # Redirect into your embedded app with host param if present.
    # If Shopify sent 'host' in the original flow, you probably stored it; fall back to Admin domain.
    host = get_cookie(request, "shopify_host")
    if not host:
        # host is base64 of "{shop}/admin"
        host = base64.b64encode(f"{shop}/admin".encode()).decode()

    # Send merchants to your app’s main page; inside embedded, App Bridge will use ?host=
    return RedirectResponse(url=f"{APP_URL}/app?shop={shop}&host={urlparse.quote(host)}")
