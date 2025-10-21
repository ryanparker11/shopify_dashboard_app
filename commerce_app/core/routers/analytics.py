from fastapi import APIRouter, HTTPException
from commerce_app.core.db import get_conn

router = APIRouter()

@router.get("/orders/summary")
async def orders_summary(shop_domain: str):
    sql = """
    SELECT
      COUNT(*)::int                         AS total_orders,
      COALESCE(SUM(total_price),0)::numeric AS total_revenue,
      ROUND(AVG(NULLIF(total_price,0))::numeric,2) AS avg_order_value
    FROM shopify.orders
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain,))
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(404, "Shop not found")
            total_orders, total_revenue, aov = row
            return {
                "total_orders": total_orders,
                "total_revenue": float(total_revenue),
                "avg_order_value": float(aov) if aov is not None else 0.0,
            }

@router.get("/orders/revenue-by-day")
async def revenue_by_day(shop_domain: str, days: int = 30):
    sql = """
    SELECT order_date::text, COALESCE(gross_revenue,0)::numeric
    FROM shopify.v_order_daily
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
      AND order_date >= current_date - %s::int
    ORDER BY order_date;
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, days))
            rows = await cur.fetchall()
            return [{"date": d, "revenue": float(r)} for d, r in rows]
