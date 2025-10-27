from fastapi import APIRouter, HTTPException
from commerce_app.core.db import get_conn
from typing import List, Dict, Any
from collections import defaultdict

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


@router.get("/charts/{shop_domain}")
async def get_charts(shop_domain: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Generate Plotly chart data for a specific shop
    """
    try:
        charts = []
        
        async with get_conn() as conn:
            # Chart 1: Monthly Revenue (Bar Chart)
            monthly_sql = """
            SELECT 
                TO_CHAR(created_at, 'YYYY-MM') as month,
                COALESCE(SUM(total_price), 0)::numeric as revenue
            FROM shopify.orders
            WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
              AND created_at >= NOW() - INTERVAL '12 months'
            GROUP BY TO_CHAR(created_at, 'YYYY-MM')
            ORDER BY month;
            """
            async with conn.cursor() as cur:
                await cur.execute(monthly_sql, (shop_domain,))
                monthly_data = await cur.fetchall()
                
                if monthly_data:
                    charts.append({
                        "data": [{
                            "x": [row[0] for row in monthly_data],
                            "y": [float(row[1]) for row in monthly_data],
                            "type": "bar",
                            "name": "Monthly Revenue",
                            "marker": {"color": "#008060"}
                        }],
                        "layout": {
                            "title": "Revenue Over Time (Last 12 Months)",
                            "xaxis": {"title": "Month"},
                            "yaxis": {"title": "Revenue ($)"}
                        }
                    })
            
            # Chart 2: Top Products (Pie Chart)
            products_sql = """
            SELECT 
                li.title as product_name,
                SUM(li.quantity)::int as total_quantity
            FROM shopify.order_line_items li
            JOIN shopify.orders o ON li.order_id = o.order_id
            WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            GROUP BY li.title
            ORDER BY total_quantity DESC
            LIMIT 5;
            """
            async with conn.cursor() as cur:
                await cur.execute(products_sql, (shop_domain,))
                product_data = await cur.fetchall()
                
                if product_data:
                    charts.append({
                        "data": [{
                            "values": [row[1] for row in product_data],
                            "labels": [row[0] for row in product_data],
                            "type": "pie",
                            "hole": 0.3
                        }],
                        "layout": {
                            "title": "Top 5 Products by Quantity Sold"
                        }
                    })
            
            # Chart 3: Daily Orders (Line Chart) - Last 30 days
            daily_orders_sql = """
            SELECT 
                DATE(created_at) as order_date,
                COUNT(*)::int as order_count
            FROM shopify.orders
            WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
              AND created_at >= NOW() - INTERVAL '30 days'
            GROUP BY DATE(created_at)
            ORDER BY order_date;
            """
            async with conn.cursor() as cur:
                await cur.execute(daily_orders_sql, (shop_domain,))
                daily_data = await cur.fetchall()
                
                if daily_data:
                    charts.append({
                        "data": [{
                            "x": [row[0].strftime('%Y-%m-%d') for row in daily_data],
                            "y": [row[1] for row in daily_data],
                            "type": "scatter",
                            "mode": "lines+markers",
                            "name": "Daily Orders",
                            "line": {"color": "#5C6AC4", "width": 2},
                            "marker": {"size": 6}
                        }],
                        "layout": {
                            "title": "Orders Over Time (Last 30 Days)",
                            "xaxis": {"title": "Date"},
                            "yaxis": {"title": "Number of Orders"}
                        }
                    })
            
            # Chart 4: Revenue by Day (using your existing view)
            revenue_sql = """
            SELECT order_date::text, COALESCE(gross_revenue,0)::numeric
            FROM shopify.v_order_daily
            WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
              AND order_date >= current_date - 30
            ORDER BY order_date;
            """
            async with conn.cursor() as cur:
                await cur.execute(revenue_sql, (shop_domain,))
                revenue_data = await cur.fetchall()
                
                if revenue_data:
                    charts.append({
                        "data": [{
                            "x": [row[0] for row in revenue_data],
                            "y": [float(row[1]) for row in revenue_data],
                            "type": "scatter",
                            "mode": "lines",
                            "name": "Daily Revenue",
                            "line": {"color": "#008060", "width": 2},
                            "fill": "tozeroy"
                        }],
                        "layout": {
                            "title": "Daily Revenue (Last 30 Days)",
                            "xaxis": {"title": "Date"},
                            "yaxis": {"title": "Revenue ($)"}
                        }
                    })
        
        if not charts:
            raise HTTPException(status_code=404, detail="No data found for this shop")
        
        return {"charts": charts}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")