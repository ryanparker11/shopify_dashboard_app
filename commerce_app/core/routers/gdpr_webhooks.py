# commerce_app/core/routers/gdpr_webhooks.py
import base64, hashlib, hmac, json, os, logging
from fastapi import APIRouter, Request, Response, HTTPException, Header
from datetime import datetime
from commerce_app.core.db import get_conn

logger = logging.getLogger(__name__)
router = APIRouter()

SHOPIFY_SECRET = os.environ["SHOPIFY_API_SECRET"]

def verify_hmac(raw_body: bytes, hmac_header: str) -> bool:
    """Verify Shopify webhook HMAC signature"""
    digest = hmac.new(
        SHOPIFY_SECRET.encode("utf-8"), 
        raw_body, 
        hashlib.sha256
    ).digest()
    calc = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(calc, (hmac_header or "").strip())


async def log_gdpr_request(shop_domain: str, topic: str, payload: dict):
    """Store GDPR request for compliance audit trail"""
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO shopify.gdpr_requests 
                    (shop_domain, request_type, payload_json, received_at, processed)
                    VALUES (%s, %s, %s, %s, false)
                    """,
                    (shop_domain, topic, json.dumps(payload), datetime.utcnow())
                )
                await conn.commit()
                logger.info(f"✅ Logged GDPR request: {topic} for {shop_domain}")
    except Exception as e:
        logger.error(f"❌ Failed to log GDPR request: {e}")


@router.post("/webhooks/customers/data_request")
async def customers_data_request(
    request: Request,
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    """
    Handle customer data request (GDPR Article 15 - Right of Access).
    
    Shopify sends this when a customer requests their data.
    You must provide any customer data YOU store within 30 days.
    """
    raw_body = await request.body()
    
    # Verify webhook authenticity
    if not verify_hmac(raw_body, x_shopify_hmac_sha256):
        logger.warning(f"⚠️  Invalid HMAC for data_request from {x_shopify_shop_domain}")
        raise HTTPException(status_code=401, detail="Invalid HMAC")
    
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception:
        payload = {}
    
    # Log the request for compliance
    await log_gdpr_request(x_shopify_shop_domain, "customers/data_request", payload)
    
    # TODO: Implement data export logic
    # 1. Query your database for customer data
    # 2. Compile data into a report
    # 3. Make available to merchant (via support email or dashboard)
    # 
    # For now, we just log it. You'll manually process these requests.
    customer_id = payload.get("customer", {}).get("id")
    customer_email = payload.get("customer", {}).get("email")
    
    logger.info(
        f"📋 Data request received for customer {customer_id} "
        f"({customer_email}) from shop {x_shopify_shop_domain}"
    )
    
    # Return 200 immediately - you have 30 days to fulfill
    return Response(status_code=200)


@router.post("/webhooks/customers/redact")
async def customers_redact(
    request: Request,
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    """
    Handle customer data redaction (GDPR Article 17 - Right to Erasure).
    
    Shopify sends this 48 hours after a customer requests deletion.
    You must delete/anonymize customer PII within your database.
    """
    raw_body = await request.body()
    
    if not verify_hmac(raw_body, x_shopify_hmac_sha256):
        logger.warning(f"⚠️  Invalid HMAC for redact from {x_shopify_shop_domain}")
        raise HTTPException(status_code=401, detail="Invalid HMAC")
    
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception:
        payload = {}
    
    await log_gdpr_request(x_shopify_shop_domain, "customers/redact", payload)
    
    customer_id = payload.get("customer", {}).get("id")
    customer_email = payload.get("customer", {}).get("email")
    
    logger.info(
        f"🗑️  Redaction request for customer {customer_id} "
        f"({customer_email}) from shop {x_shopify_shop_domain}"
    )
    
    # Delete or anonymize customer data
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # Get shop_id
                await cur.execute(
                    "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                    (x_shopify_shop_domain,)
                )
                shop_row = await cur.fetchone()
                
                if shop_row:
                    shop_id = shop_row[0]
                    
                    # Anonymize customer data in your database
                    await cur.execute(
                        """
                        UPDATE shopify.customers
                        SET 
                            email = 'redacted_' || customer_id || '@redacted.com',
                            first_name = 'REDACTED',
                            last_name = 'REDACTED',
                            phone = NULL,
                            raw_json = '{}'::jsonb
                        WHERE shop_id = %s AND customer_id = %s
                        """,
                        (shop_id, customer_id)
                    )
                    
                    # Also anonymize orders from this customer
                    await cur.execute(
                        """
                        UPDATE shopify.orders
                        SET 
                            email = 'redacted_' || customer_id || '@redacted.com',
                            raw_json = jsonb_set(
                                raw_json, 
                                '{customer}', 
                                '{"id": null, "email": "redacted"}'::jsonb
                            )
                        WHERE shop_id = %s AND customer_id = %s
                        """,
                        (shop_id, customer_id)
                    )
                    
                    await conn.commit()
                    logger.info(f"✅ Redacted customer {customer_id} from shop {shop_id}")
                    
                    # Mark GDPR request as processed
                    await cur.execute(
                        """
                        UPDATE shopify.gdpr_requests
                        SET processed = true, processed_at = NOW()
                        WHERE shop_domain = %s 
                        AND request_type = 'customers/redact'
                        AND payload_json->>'customer'->>'id' = %s
                        AND NOT processed
                        """,
                        (x_shopify_shop_domain, str(customer_id))
                    )
                    await conn.commit()
                    
    except Exception as e:
        logger.error(f"❌ Failed to redact customer {customer_id}: {e}")
        # Still return 200 - log the error for manual review
    
    return Response(status_code=200)


@router.post("/webhooks/shop/redact")
async def shop_redact(
    request: Request,
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    """
    Handle shop data redaction.
    
    Shopify sends this 48 hours after a shop uninstalls your app.
    You must delete ALL shop data (or retain only anonymized aggregates).
    """
    raw_body = await request.body()
    
    if not verify_hmac(raw_body, x_shopify_hmac_sha256):
        logger.warning(f"⚠️  Invalid HMAC for shop_redact from {x_shopify_shop_domain}")
        raise HTTPException(status_code=401, detail="Invalid HMAC")
    
    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except Exception:
        payload = {}
    
    await log_gdpr_request(x_shopify_shop_domain, "shop/redact", payload)
    
    shop_id = payload.get("shop_id")
    logger.info(f"🗑️  Shop redaction request for {x_shopify_shop_domain} (ID: {shop_id})")
    
    # Delete all shop data
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # Get internal shop_id
                await cur.execute(
                    "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                    (x_shopify_shop_domain,)
                )
                shop_row = await cur.fetchone()
                
                if shop_row:
                    internal_shop_id = shop_row[0]
                    
                    # Delete in order (respecting foreign keys)
                    tables = [
                        'shopify.orders',
                        'shopify.customers', 
                        'shopify.products',
                        'shopify.webhooks_received',
                        'shopify.shops'
                    ]
                    
                    for table in tables:
                        await cur.execute(
                            f"DELETE FROM {table} WHERE shop_id = %s",
                            (internal_shop_id,)
                        )
                        logger.info(f"  Deleted from {table}")
                    
                    await conn.commit()
                    logger.info(f"✅ Completely redacted shop {x_shopify_shop_domain}")
                    
                    # Mark as processed
                    await cur.execute(
                        """
                        UPDATE shopify.gdpr_requests
                        SET processed = true, processed_at = NOW()
                        WHERE shop_domain = %s 
                        AND request_type = 'shop/redact'
                        AND NOT processed
                        """,
                        (x_shopify_shop_domain,)
                    )
                    await conn.commit()
                    
    except Exception as e:
        logger.error(f"❌ Failed to redact shop {x_shopify_shop_domain}: {e}")
        # Still return 200 - log for manual review
    
    return Response(status_code=200)
