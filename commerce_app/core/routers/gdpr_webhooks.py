# gdpr_webhooks.py
import base64, hashlib, hmac, json, os, asyncio
from fastapi import APIRouter, Request, Response, HTTPException
from starlette.background import BackgroundTasks

SHOPIFY_SECRET = os.environ["SHOPIFY_API_SECRET"]  # from Partner Dashboard

router = APIRouter()

def verify_hmac(raw_body: bytes, hmac_header: str) -> bool:
    digest = hmac.new(SHOPIFY_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    calc = base64.b64encode(digest).decode("utf-8")
    # Shopify header can include padding differences; do a timing-safe compare
    return hmac.compare_digest(calc, (hmac_header or "").strip())

async def handle_customers_data_request(payload: dict):
    # Example: log + prepare/export only what YOU store (if any).
    # If you store nothing identifiable, respond 200 and do nothing.
    # If you store PII, compile a dataset to return via merchant support (don’t respond with data here).
    customer = payload.get("customer", {})
    # e.g., queue a job: enqueue("gdpr_data_request", payload)

async def handle_customers_redact(payload: dict):
    # Delete/anonymize any PII for the given customer in your DB.
    # Keep only aggregated, non-identifiable metrics if they cannot be re-identified.
    shop_id = payload.get("shop_id")
    customer = payload.get("customer", {})
    customer_id = customer.get("id")
    # e.g., SQL: anonymize emails/names, drop addresses, null IPs for this customer_id & shop_id

async def handle_shop_redact(payload: dict):
    # Delete/anonymize ALL PII you stored for this shop (after uninstall/data request).
    # Keep aggregate statistics only if irreversible (no user-level keys).
    shop_domain = payload.get("shop_domain")
    # e.g., cascade delete from your shop tables, purge tokens/rows, revoke sessions

async def _dispatch(topic: str, payload: dict):
    if topic == "customers/data_request":
        await handle_customers_data_request(payload)
    elif topic == "customers/redact":
        await handle_customers_redact(payload)
    elif topic == "shop/redact":
        await handle_shop_redact(payload)

@router.post("/webhooks/gdpr")
async def gdpr_catch_all(request: Request, background: BackgroundTasks):
    # You can also create three separate endpoints if you prefer. This catch-all uses the header topic.
    raw = await request.body()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_hmac(raw, hmac_header):
        raise HTTPException(status_code=401, detail="Invalid HMAC")

    topic = request.headers.get("X-Shopify-Topic", "")
    try:
        payload = json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        payload = {}

    # Process in background; return 200 fast
    background.add_task(_dispatch, topic, payload)
    return Response(status_code=200)
