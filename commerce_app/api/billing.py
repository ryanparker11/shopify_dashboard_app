# === BILLING ADDITIONS START: api/billing.py ==================================
import os
import httpx
from fastapi import APIRouter, HTTPException, Request, Depends

# If you already have helpers for token lookup, import them here.
# from commerce_app.core.db import get_conn
# from commerce_app.core.auth import get_shop_access_token

router = APIRouter(prefix="/api/billing", tags=["billing"])

APP_URL = os.getenv("APP_URL", "https://your-app.example.com")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-10")

# --- Utilities ---------------------------------------------------------------
async def admin_graphql(shop_domain: str, access_token: str, query: str, variables: dict):
    url = f"https://{shop_domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, json={"query": query, "variables": variables}, headers=headers)
        r.raise_for_status()
        return r.json()

async def get_access_token_for_shop(shop_domain: str) -> str:
    """
    Replace this stub with your real token lookup (e.g., from DB/session).
    """
    # token = await get_shop_access_token(shop_domain)
    token = os.getenv("SHOP_ADMIN_ACCESS_TOKEN")  # TEMP for local testing only
    if not token:
        raise HTTPException(401, "Missing access token for shop")
    return token

# --- GraphQL ---------------------------------------------------------------
APP_SUBSCRIPTION_CREATE = """
mutation AppSubscriptionCreate($name: String!, $returnUrl: URL!, $lineItems: [AppSubscriptionLineItemInput!]!, $test: Boolean, $trialDays: Int) {
  appSubscriptionCreate(name: $name, returnUrl: $returnUrl, lineItems: $lineItems, test: $test, trialDays: $trialDays) {
    confirmationUrl
    userErrors { field message }
    appSubscription { id }
  }
}
"""

APP_INSTALLATION_QUERY = """
query {
  currentAppInstallation {
    activeSubscriptions {
      id
      name
      status
      currentPeriodEnd
      lineItems {
        plan { pricingDetails { __typename
          ... on AppRecurringPricingDetails { interval price { amount currencyCode } }
          ... on AppUsagePricingDetails { cappedAmount { amount currencyCode } terms }
        }}
      }
    }
  }
}
"""

# --- Routes -----------------------------------------------------------------
@router.post("/start")
async def start_billing(request: Request):
    """
    Creates a monthly recurring subscription and returns the confirmationUrl to redirect the merchant.
    Body JSON: { shop_domain, plan_name, price, test?, trialDays? }
    """
    body = await request.json()
    shop = body["shop_domain"]
    plan_name = body.get("plan_name", "Lodestar Pro")
    price_amount = float(body.get("price", 25))
    test = bool(body.get("test", True))
    trial_days = body.get("trialDays", 7)

    access_token = await get_access_token_for_shop(shop)
    variables = {
        "name": plan_name,
        "returnUrl": f"{APP_URL}/billing/return?shop={shop}",
        "test": test,
        "trialDays": trial_days,
        "lineItems": [{
            "plan": {
                "appRecurringPricingDetails": {
                    "price": {"amount": price_amount, "currencyCode": "USD"},
                    "interval": "EVERY_30_DAYS"
                }
            }
        }]
    }

    data = await admin_graphql(shop, access_token, APP_SUBSCRIPTION_CREATE, variables)
    payload = data.get("data", {}).get("appSubscriptionCreate", {})
    if payload.get("userErrors"):
        raise HTTPException(400, payload["userErrors"])
    return {"confirmationUrl": payload["confirmationUrl"], "subscriptionId": payload["appSubscription"]["id"]}

@router.get("/return")
async def billing_return(shop: str):
    """
    Shopify will redirect here after approval. We verify active subscriptions.
    """
    access_token = await get_access_token_for_shop(shop)
    data = await admin_graphql(shop, access_token, APP_INSTALLATION_QUERY, {})
    active = data["data"]["currentAppInstallation"]["activeSubscriptions"]
    # TODO: persist in DB if desired
    return {"ok": True, "activeSubscriptions": active}

@router.get("/status")
async def billing_status(shop: str):
    """
    Returns { isPro: bool } for gating Pro features.
    """
    access_token = await get_access_token_for_shop(shop)
    data = await admin_graphql(shop, access_token, APP_INSTALLATION_QUERY, {})
    active = data["data"]["currentAppInstallation"]["activeSubscriptions"] or []
    is_pro = any(s.get("status") in ("ACTIVE", "PENDING") for s in active)
    return {"isPro": is_pro, "activeSubscriptions": active}
# === BILLING ADDITIONS END: api/billing.py ====================================
