"""
Shopify Billing Module for Lodestar Analytics
Uses Shopify's Managed Pricing (configured in Partner Dashboard)

This module provides:
1. Subscription status checking via GraphQL
2. Redirect to Shopify's hosted pricing page
3. Billing webhook handling
4. FastAPI dependency for gating routes behind subscription
"""

import os
import json
import base64
import hmac
import hashlib
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

import httpx
from fastapi import APIRouter, Depends, HTTPException, Header, Request, BackgroundTasks
from fastapi.responses import RedirectResponse
from dotenv import load_dotenv

# Note: Update this import path based on where you place this file
# If you place it at commerce_app/billing.py, use:
# from commerce_app.core.db import get_conn
# If testing standalone, adjust accordingly
try:
    from commerce_app.core.db import get_conn
    from commerce_app.auth.session_tokens import verify_shopify_session_token
except ImportError:
    # Fallback for testing
    from core.db import get_conn
    from auth.session_tokens import verify_shopify_session_token

load_dotenv()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/billing", tags=["billing"])

# Environment variables
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET")
APP_URL = os.environ.get("APP_URL", "").rstrip("/")


class SubscriptionStatus(str, Enum):
    """Shopify subscription status enum"""
    ACTIVE = "ACTIVE"
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"
    DECLINED = "DECLINED"
    EXPIRED = "EXPIRED"
    FROZEN = "FROZEN"


class BillingError(Exception):
    """Custom exception for billing-related errors"""
    pass


# ============================================================================
# GraphQL Queries
# ============================================================================

SUBSCRIPTION_STATUS_QUERY = """
query {
    currentAppInstallation {
        activeSubscriptions {
            id
            name
            status
            test
            trialDays
            currentPeriodEnd
            createdAt
            lineItems {
                plan {
                    pricingDetails {
                        ... on AppRecurringPricing {
                            price {
                                amount
                                currencyCode
                            }
                            interval
                        }
                    }
                }
            }
        }
    }
}
"""


# ============================================================================
# Core Billing Functions
# ============================================================================

async def get_shop_access_token(shop_domain: str) -> Optional[str]:
    """
    Retrieve access token for a shop from database.
    
    Args:
        shop_domain: Shop domain (e.g., "store.myshopify.com")
    
    Returns:
        Access token string or None if not found
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT access_token FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            row = await cur.fetchone()
            return row[0] if row else None


async def check_subscription_status(
    shop: str,
    access_token: Optional[str] = None
) -> Dict[str, Any]:
    """
    Check subscription status for a shop using GraphQL.
    
    Args:
        shop: Shop domain
        access_token: Optional access token (will fetch from DB if not provided)
    
    Returns:
        Dict with subscription info:
        {
            "has_active_subscription": bool,
            "subscriptions": List[Dict],
            "is_trial": bool,
            "trial_ends": str (ISO datetime) or None
        }
    """
    if not access_token:
        access_token = await get_shop_access_token(shop)
        if not access_token:
            raise BillingError(f"No access token found for shop: {shop}")
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }
    
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                f"https://{shop}/admin/api/2025-01/graphql.json",
                json={"query": SUBSCRIPTION_STATUS_QUERY},
                headers=headers
            )
            
            if response.status_code != 200:
                logger.error(f"GraphQL error: {response.status_code} - {response.text}")
                raise BillingError(f"Failed to check subscription: {response.status_code}")
            
            data = response.json()
            
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                raise BillingError(f"GraphQL errors: {data['errors']}")
            
            subscriptions = (
                data.get("data", {})
                .get("currentAppInstallation", {})
                .get("activeSubscriptions", [])
            )
            
            # Filter to only ACTIVE subscriptions (ignore test unless in dev)
            active_subs = [
                s for s in subscriptions
                if s["status"] == SubscriptionStatus.ACTIVE
            ]
            
            # Check if any subscription is in trial period
            is_trial = False
            trial_ends = None
            for sub in active_subs:
                if sub.get("trialDays", 0) > 0:
                    is_trial = True
                    # Calculate trial end date
                    created_at = datetime.fromisoformat(
                        sub["createdAt"].replace("Z", "+00:00")
                    )
                    from datetime import timedelta
                    trial_end = created_at + timedelta(days=sub["trialDays"])
                    trial_ends = trial_end.isoformat()
                    break
            
            return {
                "has_active_subscription": len(active_subs) > 0,
                "subscriptions": active_subs,
                "is_trial": is_trial,
                "trial_ends": trial_ends
            }
    
    except httpx.HTTPError as e:
        logger.error(f"HTTP error checking subscription: {e}")
        raise BillingError(f"Network error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in check_subscription_status: {e}", exc_info=True)
        raise BillingError(f"Unexpected error: {e}")


async def get_pricing_page_url(shop: str) -> str:
    """
    Get the URL for Shopify's hosted pricing page.
    
    This URL is generated by Shopify when you set up Managed Pricing
    in your Partner Dashboard.
    
    Args:
        shop: Shop domain (e.g., "store.myshopify.com")
    
    Returns:
        URL to redirect merchant to for plan selection
    """
    # Extract shop name without .myshopify.com
    shop_name = shop.replace(".myshopify.com", "")
    
    # This is the standard format for managed pricing URLs
    # The 'lodestar-analytics' should match your app handle in Partner Dashboard
    return f"https://admin.shopify.com/store/{shop_name}/charges/lodestar-analytics/pricing_plans"


async def update_shop_subscription_status(
    shop_domain: str,
    status: str,
    plan_name: Optional[str] = None,
    subscription_id: Optional[str] = None
) -> None:
    """
    Update subscription status in database.
    
    Args:
        shop_domain: Shop domain
        status: Subscription status (ACTIVE, CANCELLED, etc.)
        plan_name: Name of the plan
        subscription_id: Shopify subscription ID
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE shopify.shops
                SET 
                    subscription_status = %s,
                    subscription_plan_name = %s,
                    subscription_id = %s,
                    subscription_updated_at = NOW()
                WHERE shop_domain = %s
                """,
                (status, plan_name, subscription_id, shop_domain)
            )
        logger.info(
            f"Updated subscription for {shop_domain}: "
            f"status={status}, plan={plan_name}"
        )


# ============================================================================
# FastAPI Dependency for Route Protection
# ============================================================================

def get_shop_from_token(payload: Dict[str, Any]) -> str:
    """
    Extract shop domain from validated session token payload.
    Same pattern as your analytics.py
    
    The 'dest' claim contains the shop URL like: https://store.myshopify.com
    """
    dest = payload.get("dest", "")
    if not dest:
        raise HTTPException(401, "Missing shop in token")
    
    # Remove https:// prefix
    shop_domain = dest.replace("https://", "").replace("http://", "")
    
    # Validate format
    if not shop_domain.endswith(".myshopify.com"):
        raise HTTPException(401, "Invalid shop domain in token")
    
    return shop_domain


async def require_active_subscription(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
) -> Dict[str, Any]:
    """
    FastAPI dependency to gate routes behind active subscription.
    
    Args:
        payload: Session token payload (auto-injected from verify_shopify_session_token)
    
    Returns:
        Subscription info dict if active
    
    Raises:
        HTTPException: 402 if no active subscription, 500 on error
    """
    shop = get_shop_from_token(payload)
    logger.info(f"🔍 Checking subscription for: {shop}")
    
    try:
        # First check database (fast, reliable after webhook updates)
        async with get_conn() as conn:
            async with conn.cursor() as cur:
                # Be explicit about column order and what we're checking
                await cur.execute(
                    """
                    SELECT 
                        subscription_status,
                        subscription_plan_name,
                        subscription_id
                    FROM shopify.shops
                    WHERE shop_domain = %s
                    """,
                    (shop,)
                )
                db_result = await cur.fetchone()
            
            logger.info(f"📊 Database result: {db_result}")
            
            # If database shows ACTIVE, trust it (webhook keeps it updated)
            if db_result:
                status = db_result[0]
                plan_name = db_result[1]
                subscription_id = db_result[2]
                
                logger.info(f"📊 Parsed - Status: {status}, Plan: {plan_name}, ID: {subscription_id}")
                
                if status == 'ACTIVE':
                    logger.info(f"✅ Active subscription (from DB) for {shop}")
                    return {
                        "has_active_subscription": True,
                        "status": status,
                        "plan_name": plan_name,
                        "subscription_id": subscription_id,
                        "source": "database"
                    }
                else:
                    logger.warning(f"⚠️ Shop {shop} has status: {status} (not ACTIVE)")
        
        # Database doesn't show ACTIVE - check with Shopify GraphQL
        logger.info(f"Checking Shopify GraphQL for {shop}")
        sub_status = await check_subscription_status(shop)
        
        if not sub_status["has_active_subscription"]:
            # No active subscription - return 402 with pricing URL
            pricing_url = await get_pricing_page_url(shop)
            logger.warning(f"❌ No active subscription for {shop}")
            raise HTTPException(
                status_code=402,
                detail={
                    "error": "subscription_required",
                    "message": "This feature requires an active subscription",
                    "pricing_url": pricing_url,
                    "has_active_subscription": False
                }
            )
        
        logger.info(f"✅ Active subscription (from GraphQL) for {shop}")
        return sub_status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking subscription for {shop}: {e}", exc_info=True)
        # On error, be permissive to avoid blocking users due to technical issues
        logger.warning(f"⚠️ Subscription check failed, allowing access (failsafe)")
        return {
            "has_active_subscription": True,
            "error": str(e),
            "fallback": True
        }


# ============================================================================
# API Routes
# ============================================================================

@router.get("/status")
async def billing_status(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    """
    Check billing status for authenticated shop.
    
    Security: Shop extracted from validated session token.
    
    Returns:
        {
            "has_active_subscription": bool,
            "subscriptions": [...],
            "is_trial": bool,
            "trial_ends": str or null
        }
    """
    try:
        shop = get_shop_from_token(payload)
        logger.info(f"🔍 Checking billing status for: {shop}")
        
        status = await check_subscription_status(shop)
        logger.info(f"✅ Billing status result: {status}")
        
        return status
    except BillingError as e:
        logger.error(f"💥 BillingError: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"💥 Unexpected error in billing_status: {e}", exc_info=True)
        # Return a safe default to avoid blocking users
        return {
            "has_active_subscription": False,
            "subscriptions": [],
            "is_trial": False,
            "trial_ends": None,
            "error": str(e)
        }


@router.get("/pricing-url")
async def get_pricing_url(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    """
    Get the Shopify-hosted pricing page URL.
    
    Security: Shop extracted from validated session token.
    
    Returns:
        {"pricing_url": "https://..."}
    """
    shop = get_shop_from_token(payload)
    return {
        "pricing_url": await get_pricing_page_url(shop)
    }


@router.get("/subscribe-url")
async def get_subscribe_url(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    """
    Get the subscription URL for the subscribe button on paywalled features.
    
    This is used by frontend components to show "Subscribe" buttons on
    tabs/features that require an active subscription.
    
    Security: Shop extracted from validated session token.
    
    Returns:
        {
            "subscribe_url": "https://...",
            "plan_name": "Lodestar Analytics Pro",
            "price": 49.00,
            "trial_days": 14
        }
    """
    shop = get_shop_from_token(payload)
    pricing_url = await get_pricing_page_url(shop)
    
    return {
        "subscribe_url": pricing_url,
        "plan_name": "Lodestar Analytics Pro",
        "price": 49.00,
        "currency": "USD",
        "trial_days": 14
    }


@router.get("/redirect-to-pricing")
async def redirect_to_pricing(
    payload: Dict[str, Any] = Depends(verify_shopify_session_token)
):
    """
    Redirect merchant to Shopify's hosted pricing page.
    
    Security: Shop extracted from validated session token.
    """
    shop = get_shop_from_token(payload)
    pricing_url = await get_pricing_page_url(shop)
    return RedirectResponse(url=pricing_url)


@router.get("/callback")
async def billing_callback(
    shop: str,
    charge_id: Optional[str] = None
):
    """
    Handle return after merchant approves/declines subscription.
    
    This is set as the "Welcome link" in Partner Dashboard pricing config.
    The Welcome link field should be: /api/billing/callback
    
    Query params:
        shop: Shop domain (required)
        charge_id: Optional charge ID from Shopify
    
    Returns:
        Redirects to main app or shows error
    """
    try:
        # Verify subscription is now active
        sub_status = await check_subscription_status(shop)
        
        if sub_status["has_active_subscription"]:
            # Update database
            if sub_status["subscriptions"]:
                first_sub = sub_status["subscriptions"][0]
                await update_shop_subscription_status(
                    shop_domain=shop,
                    status=first_sub["status"],
                    plan_name=first_sub["name"],
                    subscription_id=first_sub["id"]
                )
            
            # Redirect to main app (update this URL to match your frontend)
            # The shop param will let your app know which store just subscribed
            return RedirectResponse(
                url=f"https://app.lodestaranalytics.io?shop={shop}&subscription=active"
            )
        else:
            # Subscription not activated (merchant declined or error)
            return RedirectResponse(
                url=f"https://app.lodestaranalytics.io?shop={shop}&subscription=declined"
            )
    
    except BillingError as e:
        logger.error(f"Error in billing callback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Webhook Handler
# ============================================================================

def verify_billing_webhook(body: bytes, hmac_header: str, secret: str) -> bool:
    """
    Verify Shopify billing webhook HMAC signature.
    Same verification as regular webhooks.
    """
    computed_hmac = base64.b64encode(
        hmac.new(
            secret.encode('utf-8'),
            body,
            hashlib.sha256
        ).digest()
    ).decode('utf-8')
    
    return hmac.compare_digest(computed_hmac, hmac_header)


async def process_subscription_webhook(
    shop_domain: str,
    topic: str,
    payload: Dict[str, Any]
) -> None:
    """
    Process app_subscriptions/update webhook.
    
    This webhook fires when:
    - Merchant approves subscription
    - Subscription status changes (cancelled, frozen, etc.)
    - Capped amount changes (for usage-based billing)
    """
    app_subscription = payload.get("app_subscription", {})
    
    subscription_id = app_subscription.get("id")
    status = app_subscription.get("status")
    plan_name = app_subscription.get("name")
    
    logger.info(
        f"Processing subscription webhook for {shop_domain}: "
        f"status={status}, plan={plan_name}"
    )
    
    # Update database
    await update_shop_subscription_status(
        shop_domain=shop_domain,
        status=status,
        plan_name=plan_name,
        subscription_id=str(subscription_id)
    )
    
    # Additional business logic based on status
    if status == SubscriptionStatus.CANCELLED:
        logger.warning(f"Subscription cancelled for {shop_domain}")
        # TODO: Send cancellation email, update feature access, etc.
    
    elif status == SubscriptionStatus.ACTIVE:
        logger.info(f"Subscription activated for {shop_domain}")
        # TODO: Send welcome email, enable full features, etc.
    
    elif status == SubscriptionStatus.FROZEN:
        logger.warning(f"Subscription frozen for {shop_domain}")
        # TODO: Limit feature access, send payment reminder, etc.


@router.post("/webhooks/app-subscriptions-update")
async def handle_subscription_update_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_shopify_topic: str = Header(...),
    x_shopify_shop_domain: str = Header(...),
    x_shopify_hmac_sha256: str = Header(...)
):
    """
    Handle app_subscriptions/update webhook from Shopify.
    
    Register this webhook in your auth/shopify_oauth.py register_webhooks():
    {
        "topic": "app_subscriptions/update",
        "address": f"{APP_URL}/billing/webhooks/app-subscriptions-update"
    }
    """
    # Get raw body for HMAC verification
    body = await request.body()
    
    # Verify webhook authenticity
    if not SHOPIFY_API_SECRET:
        raise HTTPException(500, "SHOPIFY_API_SECRET not configured")
    
    if not verify_billing_webhook(body, x_shopify_hmac_sha256, SHOPIFY_API_SECRET):
        logger.warning(f"Invalid webhook signature from {x_shopify_shop_domain}")
        raise HTTPException(401, "Invalid webhook signature")
    
    # Parse payload
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON payload")
    
    # Process in background
    background_tasks.add_task(
        process_subscription_webhook,
        x_shopify_shop_domain,
        x_shopify_topic,
        payload
    )
    
    return {"status": "ok"}


# ============================================================================
# Database Migration Helper
# ============================================================================

async def ensure_billing_columns():
    """
    Ensure billing-related columns exist in shops table.
    Run this once during deployment or add to your migration system.
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                -- Add billing columns if they don't exist
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_schema = 'shopify' 
                        AND table_name = 'shops' 
                        AND column_name = 'subscription_status'
                    ) THEN
                        ALTER TABLE shopify.shops
                        ADD COLUMN subscription_status VARCHAR(50),
                        ADD COLUMN subscription_plan_name VARCHAR(255),
                        ADD COLUMN subscription_id VARCHAR(255),
                        ADD COLUMN subscription_updated_at TIMESTAMP;
                    END IF;
                END $$;
            """)
        logger.info("✅ Billing columns ensured in shops table")


# ============================================================================
# Utility Functions
# ============================================================================

async def get_all_shops_with_expired_trials() -> List[Dict[str, Any]]:
    """
    Get all shops whose trial period has expired but haven't converted.
    Useful for sending reminder emails or limiting access.
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    shop_id,
                    shop_domain,
                    shop_name,
                    subscription_status,
                    installed_at
                FROM shopify.shops
                WHERE subscription_status IS NULL
                   OR subscription_status != 'ACTIVE'
            """)
            return await cur.fetchall()


async def get_subscription_metrics() -> Dict[str, Any]:
    """
    Get subscription metrics for business analytics.
    
    Returns:
        {
            "total_shops": int,
            "active_subscriptions": int,
            "trial_subscriptions": int,
            "cancelled_subscriptions": int,
            "conversion_rate": float
        }
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT 
                    COUNT(*) as total_shops,
                    COUNT(*) FILTER (WHERE subscription_status = 'ACTIVE') as active,
                    COUNT(*) FILTER (WHERE subscription_status = 'CANCELLED') as cancelled,
                    COUNT(*) FILTER (WHERE subscription_status IS NULL) as no_subscription
                FROM shopify.shops
            """)
            row = await cur.fetchone()
            
            total = row[0] or 0
            active = row[1] or 0
            cancelled = row[2] or 0
            no_sub = row[3] or 0
            
            # Estimate trial subscriptions (installed but no status)
            trial = no_sub
            
            # Calculate conversion rate
            conversion_rate = (active / total * 100) if total > 0 else 0
            
            return {
                "total_shops": total,
                "active_subscriptions": active,
                "trial_subscriptions": trial,
                "cancelled_subscriptions": cancelled,
                "conversion_rate": round(conversion_rate, 2)
            }


# ============================================================================
# Example Usage in Other Routes
# ============================================================================

"""
Example: Protecting a route with subscription requirement

from commerce_app.billing import require_active_subscription
from fastapi import Depends

@app.get("/api/analytics/premium-feature")
async def premium_feature(
    subscription: Dict = Depends(require_active_subscription),
    shop: str = None
):
    # This route is only accessible with active subscription
    # If no subscription, user gets 402 error with pricing_url
    
    if subscription["is_trial"]:
        return {"message": "Trial user", "trial_ends": subscription["trial_ends"]}
    else:
        return {"message": "Paid user", "data": "premium analytics"}


Example: Manual subscription check in route

from commerce_app.billing import check_subscription_status, get_pricing_page_url

@app.get("/api/data")
async def get_data(shop: str):
    sub_status = await check_subscription_status(shop)
    
    if not sub_status["has_active_subscription"]:
        pricing_url = await get_pricing_page_url(shop)
        raise HTTPException(
            status_code=402,
            detail={
                "message": "Subscription required",
                "pricing_url": pricing_url
            }
        )
    
    return {"data": "your data here"}
"""