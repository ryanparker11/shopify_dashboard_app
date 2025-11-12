from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from commerce_app.core.db import get_conn
from typing import List, Dict, Any
from io import BytesIO
from datetime import datetime
import pandas as pd

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
                "total_orders": int(total_orders),
                "total_revenue": float(total_revenue) if total_revenue is not None else 0.0,
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
    Generate Plotly chart data for a specific shop, and include export URLs.
    """
    try:
        charts: List[Dict[str, Any]] = []

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
                        "key": "monthly_revenue",
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
                        },
                        "export_url": f"/charts/{shop_domain}/export/monthly_revenue"
                    })

            # Chart 2: Top 5 Products (% of Total Revenue) with "Other"
            total_rev_sql = """
            SELECT COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_rev
            FROM shopify.order_line_items li
            JOIN shopify.orders o ON li.order_id = o.order_id
            WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
            """
            top5_sql = """
            SELECT 
                li.title AS product_name,
                COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_revenue
            FROM shopify.order_line_items li
            JOIN shopify.orders o ON li.order_id = o.order_id
            WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            GROUP BY li.title
            ORDER BY total_revenue DESC
            LIMIT 5;
            """
            async with conn.cursor() as cur:
                # total shop revenue across ALL products
                await cur.execute(total_rev_sql, (shop_domain,))
                total_row = await cur.fetchone()
                total_rev = float(total_row[0]) if total_row and total_row[0] is not None else 0.0

                # top 5 products by revenue
                await cur.execute(top5_sql, (shop_domain,))
                top5_rows = await cur.fetchall()

                if total_rev > 0 and top5_rows:
                    labels = [r[0] for r in top5_rows]
                    values = [float(r[1]) for r in top5_rows]

                    other = max(total_rev - sum(values), 0.0)
                    if other > 0.000001:  # avoid tiny rounding negatives
                        labels.append("Other")
                        values.append(other)

                    charts.append({
                        "key": "top_products_revenue",
                        "data": [{
                            "values": values,              # raw revenue incl. "Other"
                            "labels": labels,
                            "type": "pie",
                            "hole": 0.3,
                            "textinfo": "label+percent",
                            "insidetextorientation": "auto"
                        }],
                        "layout": {
                            "title": "Top 5 Products (% of Revenue)"
                        },
                        "export_url": f"/charts/{shop_domain}/export/top_products_revenue"
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
                        "key": "daily_orders_30d",
                        "data": [{
                            "x": [row[0].strftime('%Y-%m-%d') for row in daily_data],
                            "y": [int(row[1]) for row in daily_data],
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
                        },
                        "export_url": f"/charts/{shop_domain}/export/daily_orders_30d"
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
                        "key": "daily_revenue_30d",
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
                        },
                        "export_url": f"/charts/{shop_domain}/export/daily_revenue_30d"
                    })

        if not charts:
            raise HTTPException(status_code=404, detail="No data found for this shop")

        return {"charts": charts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")


@router.get("/charts/{shop_domain}/export/{chart_key}")
async def export_chart_excel(shop_domain: str, chart_key: str):
    """
    Export the underlying dataset for a chart as an Excel file.
    Valid chart_key values:
      - monthly_revenue
      - top_products_revenue
      - daily_orders_30d
      - daily_revenue_30d
    """
    try:
        async with get_conn() as conn:
            if chart_key == "monthly_revenue":
                sql = """
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
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                df = pd.DataFrame(rows, columns=["month", "revenue"])

            elif chart_key == "top_products_revenue":
                total_sql = """
                SELECT COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_rev
                FROM shopify.order_line_items li
                JOIN shopify.orders o ON li.order_id = o.order_id
                WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
                """
                top5_sql = """
                SELECT li.title AS product_name, COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_revenue
                FROM shopify.order_line_items li
                JOIN shopify.orders o ON li.order_id = o.order_id
                WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                GROUP BY li.title
                ORDER BY total_revenue DESC
                LIMIT 5;
                """
                async with conn.cursor() as cur:
                    await cur.execute(total_sql, (shop_domain,))
                    total = float((await cur.fetchone())[0] or 0.0)
                    await cur.execute(top5_sql, (shop_domain,))
                    top5 = await cur.fetchall()

                labels = [r[0] for r in top5]
                values = [float(r[1]) for r in top5]
                other = max(total - sum(values), 0.0)
                if other > 0.000001:
                    labels.append("Other")
                    values.append(other)

                pct = [(v / total) * 100.0 if total else 0.0 for v in values]
                df = pd.DataFrame({
                    "product_name": labels,
                    "revenue": values,
                    "percent_of_total": pct
                })

            elif chart_key == "daily_orders_30d":
                sql = """
                SELECT DATE(created_at) as order_date, COUNT(*)::int as order_count
                FROM shopify.orders
                WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                  AND created_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE(created_at)
                ORDER BY order_date;
                """
                async with conn.cursor() as cur:
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                df = pd.DataFrame(rows, columns=["order_date", "order_count"])

            elif chart_key == "daily_revenue_30d":
                sql = """
                SELECT order_date::date, COALESCE(gross_revenue,0)::numeric
                FROM shopify.v_order_daily
                WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                  AND order_date >= current_date - 30
                ORDER BY order_date;
                """
                async with conn.cursor() as cur:
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                df = pd.DataFrame(rows, columns=["order_date", "revenue"])

            else:
                raise HTTPException(404, f"Unknown chart_key: {chart_key}")

        # Build Excel in-memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        output.seek(0)

        filename = f"{chart_key}_{datetime.utcnow().strftime('%Y%m%d')}.xlsx"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")