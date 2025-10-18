# commerce_app/api/analytics.py
from fastapi import APIRouter
from datetime import date, timedelta

router = APIRouter(prefix="/api", tags=["analytics"])


@router.get("/orders/summary")
def orders_summary():
    # mock data for now
    return {
        "total_orders": 4213,
        "total_revenue": 198345.77,
        "avg_order_value": 47.08
    }


@router.get("/orders/revenue-by-day")
def revenue_by_day(days: int = 30):
    out = []
    for i in range(days):
        d = date.today() - timedelta(days=days - i)
        out.append({"date": d.isoformat(), "revenue": 3000 + (i * 37) % 900})
    return out
