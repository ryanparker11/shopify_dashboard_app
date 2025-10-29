from fastapi import APIRouter, Header, Request, HTTPException, BackgroundTasks
from commerce_app.core.db import get_conn
import json
import hmac
import hashlib
import base64
from typing import Optional
import os
import traceback

router = APIRouter()

def verify_webhook(body: bytes, hmac_header: str, secret: str) -> bool:
    """
    Verify Shopify webhook HMAC signature.
    
    Args:
        body: Raw request body bytes
        hmac_header: X-Shopify-Hmac-Sha256 header value
        secret: Your Shopify webhook secret (from app settings)
    
    Returns:
        True if signature is valid, False otherwise
    """
    computed_hmac = base64.b64encode(
        hmac.new(
            secret.encode('utf-8'),
            body,
            hashlib.sha256
        ).digest()
    ).decode('utf-8')
    
    return hmac.compare_digest(computed_hmac, hmac_header)


async def process_webhook(shop_domain: str, topic: str, payload: dict):
    """
    Process webhook payload and update relevant tables.
    This runs in the background after the webhook response is sent.
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            try:
                # Get shop_id for later use
                await cur.execute(
                    "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                    (shop_domain,)
                )
                shop_row = await cur.fetchone()
                if not shop_row:
                    print(f"⚠️  Warning: Shop {shop_domain} not found in database")
                    return
                
                shop_id = shop_row[0]
                entity_id = payload.get("id")  # Order/product/customer ID
                
                # Route to appropriate handler based on topic
                if topic == "orders/create" or topic == "orders/updated":
                    await process_order_webhook(cur, shop_id, payload)
                elif topic == "products/create" or topic == "products/update":
                    await process_product_webhook(cur, shop_id, payload)
                elif topic == "customers/create" or topic == "customers/update":
                    await process_customer_webhook(cur, shop_id, payload)
                else:
                    print(f"⚠️  Unknown webhook topic: {topic}")
                
                await conn.commit()
                
                # Mark webhook as processed
                await cur.execute(
                    """
                    UPDATE shopify.webhooks_received 
                    SET processed = true 
                    WHERE shop_id = %s 
                      AND topic = %s 
                      AND payload_json->>'id' = %s
                      AND processed = false
                    """,
                    (shop_id, topic, str(entity_id))
                )
                await conn.commit()
                
                print(f"✅ Webhook processed: {topic} for ID {entity_id}")
                
            except Exception as e:
                print(f"❌ Error processing webhook: {e}")
                traceback.print_exc()
                await conn.rollback()


async def process_order_webhook(cur, shop_id: int, payload: dict):
    """
    Process orders/create and orders/updated webhooks.
    Now includes: email, order_number, line_items, raw_json
    """
    order_id = payload.get("id")
    
    # Extract customer info
    customer_id = None
    email = None
    if payload.get("customer"):
        customer_id = payload.get("customer", {}).get("id")
        email = payload.get("email") or payload.get("customer", {}).get("email")
    else:
        email = payload.get("email")
    
    # Extract order number (can be in different formats)
    order_number = payload.get("order_number")
    if not order_number and payload.get("name"):
        # Remove the # prefix from name if present
        order_number = payload.get("name").replace("#", "")
    
    # Extract shipping price with fallback logic
    shipping_price = "0.00"
    if payload.get("total_shipping_price_set"):
        shipping_price = payload.get("total_shipping_price_set", {}).get("shop_money", {}).get("amount", "0.00")
    elif payload.get("shipping_price"):
        shipping_price = payload.get("shipping_price")
    
    # Upsert order data with ALL fields
    await cur.execute(
        """
        INSERT INTO shopify.orders (
            shop_id,
            order_id,
            customer_id,
            email,
            name,
            order_number,
            processed_at,
            financial_status,
            fulfillment_status,
            currency,
            subtotal_price,
            total_discounts,
            total_tax,
            shipping_price,
            total_price,
            line_items,
            raw_json,
            created_at,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (shop_id, order_id) 
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            email = EXCLUDED.email,
            name = EXCLUDED.name,
            order_number = EXCLUDED.order_number,
            processed_at = EXCLUDED.processed_at,
            financial_status = EXCLUDED.financial_status,
            fulfillment_status = EXCLUDED.fulfillment_status,
            currency = EXCLUDED.currency,
            subtotal_price = EXCLUDED.subtotal_price,
            total_discounts = EXCLUDED.total_discounts,
            total_tax = EXCLUDED.total_tax,
            shipping_price = EXCLUDED.shipping_price,
            total_price = EXCLUDED.total_price,
            line_items = EXCLUDED.line_items,
            raw_json = EXCLUDED.raw_json,
            updated_at = EXCLUDED.updated_at;
        """,
        (
            shop_id,
            order_id,
            customer_id,
            email,
            payload.get("name"),  # Order name like "#1001"
            order_number,
            payload.get("processed_at"),
            payload.get("financial_status"),
            payload.get("fulfillment_status"),
            payload.get("currency", "USD"),
            payload.get("subtotal_price", "0.00"),
            payload.get("total_discounts", "0.00"),
            payload.get("total_tax", "0.00"),
            shipping_price,
            payload.get("total_price", "0.00"),
            json.dumps(payload.get("line_items", [])),  # Store product info
            json.dumps(payload),  # Store complete webhook for debugging
            payload.get("created_at"),
            payload.get("updated_at")
        )
    )
    
    print(f"✅ Processed order {payload.get('name')} - ${payload.get('total_price')} from {email}")


async def process_product_webhook(cur, shop_id: int, payload: dict):
    """Process products/create and products/update webhooks."""
    product_id = payload.get("id")
    
    await cur.execute(
        """
        INSERT INTO shopify.products (
            shop_id,
            product_id,
            title,
            handle,
            vendor,
            product_type,
            tags,
            status,
            created_at,
            updated_at,
            raw_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (shop_id, product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            handle = EXCLUDED.handle,
            vendor = EXCLUDED.vendor,
            product_type = EXCLUDED.product_type,
            tags = EXCLUDED.tags,
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at,
            raw_json = EXCLUDED.raw_json;
        """,
        (
            shop_id,
            product_id,
            payload.get("title"),
            payload.get("handle"),
            payload.get("vendor"),
            payload.get("product_type"),
            payload.get("tags"),
            payload.get("status"),
            payload.get("created_at"),
            payload.get("updated_at"),
            json.dumps(payload)
        )
    )


async def process_customer_webhook(cur, shop_id: int, payload: dict):
    """Process customers/create and customers/update webhooks."""
    customer_id = payload.get("id")
    
    await cur.execute(
        """
        INSERT INTO shopify.customers (
            shop_id,
            customer_id,
            email,
            first_name,
            last_name,
            phone,
            total_spent,
            orders_count,
            state,
            created_at,
            updated_at,
            raw_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (shop_id, customer_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            phone = EXCLUDED.phone,
            total_spent = EXCLUDED.total_spent,
            orders_count = EXCLUDED.orders_count,
            state = EXCLUDED.state,
            updated_at = EXCLUDED.updated_at,
            raw_json = EXCLUDED.raw_json;
        """,
        (
            shop_id,
            customer_id,
            payload.get("email"),
            payload.get("first_name"),
            payload.get("last_name"),
            payload.get("phone"),
            payload.get("total_spent"),
            payload.get("orders_count"),
            payload.get("state"),
            payload.get("created_at"),
            payload.get("updated_at"),
            json.dumps(payload)
        )
    )


@router.post("/ingest")
async def webhook_ingest(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_topic: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    """
    Ingest Shopify webhooks.
    
    1. Verifies HMAC signature
    2. Stores raw webhook in webhooks_received table
    3. Processes webhook in background to update relevant tables
    
    Returns 200 OK quickly to satisfy Shopify's timeout requirements.
    """
    # Get raw body for HMAC verification
    body = await request.body()

    # Verify webhook authenticity
    # Use SHOPIFY_API_SECRET (same secret for OAuth and webhooks)
    webhook_secret = os.getenv("SHOPIFY_API_SECRET")
    if not webhook_secret:
        raise HTTPException(500, "SHOPIFY_API_SECRET not configured")
    
    if not verify_webhook(body, x_shopify_hmac_sha256, webhook_secret):
        raise HTTPException(401, "Invalid webhook signature")
    
    # Parse JSON payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON payload")

    # Store raw webhook immediately
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # Check if shop exists
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (x_shopify_shop_domain,)
            )
            shop_row = await cur.fetchone()
            
            if not shop_row:
                # Log the webhook but don't fail - shop might be registering
                print(f"Warning: Received webhook for unregistered shop: {x_shopify_shop_domain}")
                # Store it anyway for debugging
                await cur.execute(
                    """
                    INSERT INTO shopify.webhooks_received (shop_id, topic, payload_json, processed)
                    VALUES (NULL, %s, %s::jsonb, false);
                    """,
                    (x_shopify_topic, json.dumps(payload))
                )
                await conn.commit()
                return {"status": "accepted", "message": "Shop not registered"}
            
            # Store webhook
            await cur.execute(
                """
                INSERT INTO shopify.webhooks_received (shop_id, topic, payload_json, processed)
                VALUES (%s, %s, %s::jsonb, false);
                """,
                (shop_row[0], x_shopify_topic, json.dumps(payload))
            )
            await conn.commit()
    
    # Process webhook in background (after returning response to Shopify)
    background_tasks.add_task(
        process_webhook,
        x_shopify_shop_domain,
        x_shopify_topic,
        payload
    )
    
    return {"status": "ok"}


@router.post("/test-ingest")
async def test_webhook_ingest(
    request: Request,
    x_shopify_topic: str = Header(...),
    x_shopify_shop_domain: str = Header(...)
):
    """Test endpoint without HMAC verification.
    REMOVE IN PRODUCTION!"""
    payload = await request.json()
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (x_shopify_shop_domain,)
            )
            shop_row = await cur.fetchone()
            
            if not shop_row:
                return {"status": "accepted", "message": "Shop not registered"}
            
            shop_id = shop_row[0]
            
            # Store webhook as processed
            await cur.execute(
                """
                INSERT INTO shopify.webhooks_received (shop_id, topic, payload_json, processed)
                VALUES (%s, %s, %s::jsonb, true);
                """,
                (shop_id, x_shopify_topic, json.dumps(payload))
            )
            
            # Process order if it's an order webhook
            if x_shopify_topic in ["orders/create", "orders/updated"]:
                await cur.execute("""
                    INSERT INTO shopify.orders (shop_id, order_id, order_number, total_price, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    shop_id,
                    payload.get('id'),
                    payload.get('order_number'),
                    payload.get('total_price'),
                    payload.get('created_at')
                ))
            
            await conn.commit()
    
    return {"status": "ok", "message": "Webhook processed"}


@router.get("/status")
async def webhook_status(shop_domain: Optional[str] = None, limit: int = 100):
    """
    Check webhook processing status.
    Useful for debugging and monitoring.
    """
    try:
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                if shop_domain:
                    await cur.execute(
                        """
                        SELECT 
                            w.id,
                            w.topic,
                            w.received_at,
                            w.processed,
                            s.shop_domain
                        FROM shopify.webhooks_received w
                        LEFT JOIN shopify.shops s ON w.shop_id = s.shop_id
                        WHERE s.shop_domain = %s
                        ORDER BY w.received_at DESC
                        LIMIT %s;
                        """,
                        (shop_domain, limit)
                    )
                else:
                    await cur.execute(
                        """
                        SELECT 
                            w.id,
                            w.topic,
                            w.received_at,
                            w.processed,
                            s.shop_domain
                        FROM shopify.webhooks_received w
                        LEFT JOIN shopify.shops s ON w.shop_id = s.shop_id
                        ORDER BY w.received_at DESC
                        LIMIT %s;
                        """,
                        (limit,)
                    )
                
                rows = await cur.fetchall()
                return [
                    {
                        "webhook_id": row[0],
                        "topic": row[1],
                        "received_at": row[2].isoformat() if row[2] else None,
                        "processed": row[3],
                        "shop_domain": row[4]
                    }
                    for row in rows
                ]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))




