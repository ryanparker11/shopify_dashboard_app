import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

import os, uvicorn, math, logging
from fastapi import FastAPI, Request, Depends               # CHANGED: add Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from commerce_app.integrations.shopify.shopify_client import get_orders, get_customers
from commerce_app.core.routers.analytics import router as analytics_router
from commerce_app.core.db import init_pool, close_pool
from commerce_app.core.routers import webhooks, health, analytics
from commerce_app.auth.shopify_oauth import router as shopify_auth
from commerce_app.core.routers import cogs
from commerce_app.core.routers.gdpr_webhooks import router as gdpr_router

# ★ NEW: session token verifier
from commerce_app.auth.session_tokens import verify_shopify_session_token   # ADDED

app = FastAPI()
templates = Jinja2Templates(directory="commerce_app/ui")

# ADD THIS: Security headers middleware for Shopify embedding
from starlette.middleware.base import BaseHTTPMiddleware

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        # CRITICAL: Allow Shopify to embed your app in an iframe
        response.headers["Content-Security-Policy"] = (
            "frame-ancestors https://admin.shopify.com https://*.myshopify.com"
        )
        return response

# Add security headers middleware FIRST
app.add_middleware(SecurityHeadersMiddleware)

# Then add CORS
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://app.lodestaranalytics.io",
        "http://localhost:5173"  # for local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],   # includes Authorization
)

# -------------------------------
# Router registration
# -------------------------------

# ✅ Protect API routers with session tokens
app.include_router(
    analytics_router,
    prefix="/api",
    tags=["analytics"],
    dependencies=[Depends(verify_shopify_session_token)]      # ADDED
)

app.include_router(
    cogs.router,
    prefix="/api",
    tags=["cogs"],
    dependencies=[Depends(verify_shopify_session_token)]      # ADDED
)

# ❗ Do NOT protect webhooks with session tokens (they use HMAC headers)
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(gdpr_router, prefix="/webhooks", tags=["gdpr"])

# ❗ OAuth routes must remain public
app.include_router(health.router)
app.include_router(shopify_auth)

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# --- Optional: a simple protected probe reviewers can hit from your UI ---
@app.get("/api/me", tags=["auth-probe"], dependencies=[Depends(verify_shopify_session_token)])  # ADDED
def me(payload = Depends(verify_shopify_session_token)):
    # payload["sub"] is the shop's numeric id; dest is the shop URL
    return {"ok": True, "shop": payload.get("dest"), "sub": payload.get("sub")}

# at bottom of commerce_app/app.py
for r in app.routes:
    logging.warning("ROUTE %s %s", getattr(r, "path", ""), getattr(r, "methods", ""))

BUILD_ID = os.environ.get("BUILD_ID", "dev")

@app.get("/whoami")
def whoami():
    return {"module": "commerce_app.app", "build_id": BUILD_ID}
