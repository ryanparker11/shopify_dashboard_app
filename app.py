import sys
from pathlib import Path

# Add the project root to PYTHONPATH at runtime
sys.path.append(str(Path(__file__).resolve().parents[3]))  # adjust depth if needed


import os, uvicorn, math
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from integrations.shopify.shopify_client import get_orders, get_customers

app = FastAPI()
templates = Jinja2Templates(directory="shopify-dashboard-app/ui")

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    orders = await get_orders(limit=20)
    customers = await get_customers(limit=20)
    revenue = sum(float(o.get("total_price", 0) or 0) for o in orders)
    avg_order = (revenue / len(orders)) if orders else 0
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "orders": orders, "customers": customers,
         "revenue": revenue, "avg_order": avg_order}
    )

if __name__ == "__main__":
    uvicorn.run("app:dashboard", host="0.0.0.0", port=8000, factory=False)
