from fastapi import APIRouter, Header, Request
from commerce_app.core.db import get_conn
import json

router = APIRouter()

@router.post("/ingest")
async def webhook_ingest(
    request: Request,
    x_shopify_topic: str = Header(...),
    x_shopify_shop_domain: str = Header(...)
):
    payload = await request.json()
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO shopify.webhooks_received (shop_id, topic, payload_json)
                VALUES (
                  (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s),
                  %s,
                  %s::jsonb
                );
                """,
                (x_shopify_shop_domain, x_shopify_topic, json.dumps(payload)),
            )
    # (Optional) return 202 and process on a background worker
    return {"status": "ok"}
