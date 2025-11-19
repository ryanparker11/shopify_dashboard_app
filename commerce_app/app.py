import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

import os, uvicorn, math, logging
from fastapi import FastAPI, Request, Depends, HTTPException, Header
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from commerce_app.integrations.shopify.shopify_client import get_orders, get_customers
from commerce_app.core.routers.analytics import router as analytics_router
from commerce_app.core.db import init_pool, close_pool
from commerce_app.core.routers import webhooks, health, analytics
from commerce_app.auth.shopify_oauth import router as shopify_auth
from commerce_app.core.routers import cogs
from commerce_app.core.routers.gdpr_webhooks import router as gdpr_router
from commerce_app.core.routers import forecasts as forecasts_router

# Session token verifier
from commerce_app.auth.session_tokens import verify_shopify_session_token

app = FastAPI()
templates = Jinja2Templates(directory="commerce_app/ui")

# Security headers middleware for Shopify embedding
from starlette.middleware.base import BaseHTTPMiddleware

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # CRITICAL: Allow Shopify to embed your app in an iframe
        response.headers["Content-Security-Policy"] = (
            "frame-ancestors https://admin.shopify.com https://*.myshopify.com"
        )
        if "X-Frame-Options" in response.headers:
            del response.headers["X-Frame-Options"]
        return response

# Add security headers middleware FIRST
app.add_middleware(SecurityHeadersMiddleware)

# Then add CORS
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://app.lodestaranalytics.io",
        "https://admin.shopify.com",
        "http://localhost:5173"  # for local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],   # includes Authorization
    expose_headers=["X-Shopify-Retry-Invalid-Session-Request"],  # Allow frontend to see retry header
)

# -------------------------------
# Router registration
# -------------------------------

# ✅ Protect API routers with session tokens
app.include_router(
    analytics_router,
    prefix="/api",
    tags=["analytics"],
    dependencies=[Depends(verify_shopify_session_token)]
)

app.include_router(
    cogs.router,
    prefix="/api",
    tags=["cogs"],
    dependencies=[Depends(verify_shopify_session_token)]
)

app.include_router(
    forecasts_router,
    prefix="/api",
    tags=["forecasts"],
    dependencies=[Depends(verify_shopify_session_token)]
)

# ❗ Do NOT protect webhooks with session tokens (they use HMAC headers)
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(gdpr_router, prefix="/webhooks", tags=["gdpr"])

# ❗ OAuth routes and health checks must remain public
app.include_router(health.router)
app.include_router(shopify_auth)  # OAuth routes at /auth/*

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# Optional: protected endpoint to verify session tokens are working
@app.get("/api/me", tags=["auth-probe"])
def me(payload = Depends(verify_shopify_session_token)):
    """
    Test endpoint to verify session token authentication is working.
    Returns shop info from the verified token.
    """
    dest = payload.get("dest", "")
    shop = dest.replace("https://", "").split("/")[0]
    return {
        "ok": True, 
        "shop": shop,
        "shop_id": payload.get("sub"),
        "user_id": payload.get("sub")  # The Shopify user ID
    }

# Logging routes
for r in app.routes:
    logging.warning("ROUTE %s %s", getattr(r, "path", ""), getattr(r, "methods", ""))

BUILD_ID = os.environ.get("BUILD_ID", "dev")

@app.get("/whoami")
def whoami():
    return {"module": "commerce_app.app", "build_id": BUILD_ID}