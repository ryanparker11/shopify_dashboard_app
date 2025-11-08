import sys
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Add the project root to PYTHONPATH at runtime
#sys.path.append(str(Path(__file__).resolve().parents[3]))  # adjust depth if needed


import os, uvicorn, math
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from commerce_app.integrations.shopify.shopify_client import get_orders, get_customers
from commerce_app.core.routers.analytics import router as analytics_router
from commerce_app.core.db import init_pool, close_pool
from commerce_app.core.routers import webhooks, health, analytics
from commerce_app.auth.shopify_oauth import router as shopify_auth
from commerce_app.core.routers import cogs




app = FastAPI()
templates = Jinja2Templates(directory="commerce_app/ui")

# mount the analytics routes
app.include_router(analytics_router, prefix="/api", tags=["analytics"])
app.include_router(cogs.router, prefix="/api", tags=["cogs"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
app.include_router(health.router)
app.include_router(shopify_auth)
#app.include_router(analytics.router)


# === BILLING ADDITIONS START: include router ==================================
from commerce_app.api.billing import router as billing_router
app.include_router(billing_router)
# === BILLING ADDITIONS END: include router ====================================



from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://app.lodestaranalytics.io",
        "http://localhost:5173",  # for local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}

#@app.get("/", response_class=HTMLResponse)
#async def dashboard(request: Request):
#    orders = await get_orders(limit=20)
#    customers = await get_customers(limit=20)
#    revenue = sum(float(o.get("total_price", 0) or 0) for o in orders)
#    avg_order = (revenue / len(orders)) if orders else 0
#    return templates.TemplateResponse(
#        "dashboard.html",
#        {"request": request, "orders": orders, "customers": customers,
#         "revenue": revenue, "avg_order": avg_order}
#    )

# at bottom of commerce_app/app.py
import logging
for r in app.routes:
    logging.warning("ROUTE %s %s", getattr(r, "path", ""), getattr(r, "methods", ""))

# optional: a build stamp to prove you pulled the new image
import os
BUILD_ID = os.environ.get("BUILD_ID", "dev")
@app.get("/whoami")
def whoami():
    return {"module": "commerce_app.app", "build_id": BUILD_ID}



#if __name__ == "__main__":
#    uvicorn.run("app:dashboard", host="0.0.0.0", port=8000, factory=False)
