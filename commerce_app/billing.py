"""
Shopify Billing Module for Lodestar Analytics
Database-driven subscription validation (webhook authoritative)

This module provides:
1. Subscription status checking via local database
2. Redirect to Shopify's hosted pricing page
3. Billing webhook handling
4. FastAPI dependency for subscription-gated routes
"""

import os
import json
import base64
import hmac
import hashlib
import logging
from typing import Optional, Dict, Any, List
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Header, Request, BackgroundTasks
from fastapi.responses import RedirectResponse
from dotenv import load_dotenv
import httpx

try:
    from commerce_app.core.db import get_conn
    from commerce_app.auth.session_tokens import verify_shopify_session_token
except ImportError:
    from core.db import get_conn
    from auth.session_tokens import verify_shopify_session_token

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/billing", tags=["billing"])

SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
APP_URL = os.environ.get("APP_URL", "").rstrip("/")


class SubscriptionStatus(str, Enum):
    ACTIVE = "ACTIVE"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"
    DECLINED = "DECLINED"
    EXPIRED = "EXPIRED"
    FROZEN = "FROZEN"


class BillingError(Exception):
    pass


# ============================================================================
# **UPDATED**: DB-ONLY Billing Check (Legacy compatible)
# ============================================================================

async def check_subscription_status(shop: str) -> Dict[str, Any]:
    """
    NEW VERSION:
    - No GraphQL calls
    - Returns subscription info purely from database
    - Backward compatible with old return format
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT subscription_status,
                       subscription_plan_name,
                       subscription_id
                FROM shopify.shops
                WHERE shop_domain = %s
                """,
                (shop,)
            )
            row = await cur.fetchone()

    if not row:
        raise BillingError(f"Shop not found in DB: {shop}")

    status, plan_name, subscription_id = row

    is_active = status == "ACTIVE"

    return {
        "has_active_subscription": is_active,
        "subscriptions": [
            {
                "id": subscription_id,
                "name": plan_name,
                "status": status,
            }
        ] if is_active else [],
        "is_trial": False,        # Trials handled by Shopify managed pricing
        "trial_ends": None
    }

    # OPTIONAL FAILSAFE (COMMENTED OUT):
    # If you want to allow access during outages:
    #
    # return {
    #     "has_active_subscription": True,
    #     "fallback": True
    # }


# ============================================================================
# Pricing Page
# ============================================================================

async def get_pricing_page_url(shop: str) -> str:
    shop_name = shop.replace(".myshopify.com", "")
    return f"https://admin.shopify.com/store/{shop_name}/charges/public_test-8/pricing_plans"


# ============================================================================
# DB Update from Webhook
# ============================================================================

async def update_shop_subscription_status(
    shop_domain: str,
    status: str,
    plan_name: Optional[str] = None,
    subscription_id: Optional[str] = None
) -> None:

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE shopify.shops
                SET subscription_status = %s,
                    subscription_plan_name = %s,
                    subscription_id = %s,
                    subscription_updated_at = NOW()
                WHERE shop_domain = %s
                """,
                (status, plan_name, subscription_id, shop_domain)
            )
            await conn.commit()

    logger.info(
        f"Updated subscription for {shop_domain}: "
        f"{status} ({plan_name})"
    )


# ============================================================================
# Extract Shop Domain From Session Token
# ============================================================================

def get_shop_from_token(payload: Dict[str, Any]) -> str:
    dest = payload.get("dest", "")
    if not dest:
        raise HTTPException(401, "Missing shop in session token")

    dest = dest.replace("https://", "").replace("http://", "")

    # New admin domain handler
    if dest.startswith("admin.shopify.com"):
        parts = dest.split("/")
        try:
            store_name = parts[2]
        except IndexError:
            raise HTTPException(401, "Invalid admin.shopify.com dest format")

        return f"{store_name}.myshopify.com"

    if dest.endswith(".myshopify.com"):
        return dest

    raise HTTPException(401, f"Invalid shop domain in token: {dest}")


# ============================================================================
# Subscription Gate Dependency
# ============================================================================

async def require_active_subscription(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
) -> Dict[str, Any]:

    shop = get_shop_from_token(payload)

    try:
        status = await check_subscription_status(shop)

        if not status["has_active_subscription"]:
            pricing_url = await get_pricing_page_url(shop)
            raise HTTPException(
                status_code=402,
                detail={
                    "error": "subscription_required",
                    "message": "This feature requires an active subscription",
                    "pricing_url": pricing_url,
                    "has_active_subscription": False
                }
            )

        logger.info(f"Subscription ACTIVE for {shop}")
        return status

    except Exception as e:
        logger.error(f"Subscription check failed for {shop}: {e}")

        # FAILSAFE — COMMENTED OUT (re-enable if desired)
        #
        # return {
        #     "has_active_subscription": True,
        #     "fallback": True,
        #     "error": str(e)
        # }
        #

        raise HTTPException(500, "Subscription check failed")


# ============================================================================
# Routes
# ============================================================================

@router.get("/status")
async def billing_status(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    shop = get_shop_from_token(payload)
    return await check_subscription_status(shop)


@router.get("/pricing-url")
async def get_pricing_url(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    shop = get_shop_from_token(payload)
    return {"pricing_url": await get_pricing_page_url(shop)}


@router.get("/redirect-to-pricing")
async def redirect_to_pricing(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    shop = get_shop_from_token(payload)
    return RedirectResponse(url=await get_pricing_page_url(shop))


# ============================================================================
# Callback After Subscription Approval
# ============================================================================

@router.get("/callback")
async def billing_callback(
    shop: str,
    charge_id: Optional[str] = None
):
    try:
        status = await check_subscription_status(shop)

        if status["has_active_subscription"]:
            sub = status["subscriptions"][0]
            await update_shop_subscription_status(
                shop_domain=shop,
                status=sub["status"],
                plan_name=sub["name"],
                subscription_id=sub["id"]
            )
            return RedirectResponse(
                f"https://app.lodestaranalytics.io?shop={shop}&subscription=active"
            )

        return RedirectResponse(
            f"https://app.lodestaranalytics.io?shop={shop}&subscription=declined"
        )

    except Exception as e:
        logger.error(f"Billing callback error: {e}")
        raise HTTPException(500, "Billing callback failed")


# ============================================================================
# Webhook for Subscription Updates
# ============================================================================

def verify_billing_webhook(body: bytes, hmac_header: str, secret: str) -> bool:
    computed = base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest()
    ).decode()
    return hmac.compare_digest(computed, hmac_header)


async def process_subscription_webhook(
    shop_domain: str,
    topic: str,
    payload: Dict[str, Any]
):
    app_sub = payload.get("app_subscription", {})
    status = app_sub.get("status")
    plan_name = app_sub.get("name")
    subscription_id = str(app_sub.get("id"))

    logger.info(f"Webhook subscription update for {shop_domain}: {status}")

    await update_shop_subscription_status(
        shop_domain,
        status,
        plan_name,
        subscription_id
    )


@router.post("/webhooks/app-subscriptions-update")
async def handle_subscription_update_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_topic: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    body = await request.body()

    if not verify_billing_webhook(body, x_shopify_hmac_sha256, SHOPIFY_API_SECRET):
        raise HTTPException(401, "Invalid webhook signature")

    payload = json.loads(body)

    background_tasks.add_task(
        process_subscription_webhook,
        x_shopify_shop_domain,
        x_shopify_topic,
        payload
    )

    return {"status": "ok"}
