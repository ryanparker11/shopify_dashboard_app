from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from typing import List, Dict, Any
from io import BytesIO
from datetime import datetime
import pandas as pd

router = APIRouter()


def get_shop_from_token(payload: Dict[str, Any] = Depends(verify_shopify_session_token)) -> str:
    """
    Extract shop domain from validated session token payload.
    
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


@router.get("/orders/summary")
async def orders_summary(shop_domain: str = Depends(get_shop_from_token)):
    """
    Get orders summary for authenticated shop.
    
    Security: shop_domain extracted from validated session token.
    """
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
async def revenue_by_day(
    shop_domain: str = Depends(get_shop_from_token),
    days: int = 30
):
    """
    Get daily revenue for authenticated shop with ALL dates (including zero-revenue days).
    
    Security: shop_domain extracted from validated session token.
    """
    sql = """
    WITH date_series AS (
        SELECT generate_series(
            current_date - %s::int,
            current_date,
            '1 day'::interval
        )::date AS order_date
    )
    SELECT 
        ds.order_date::text,
        COALESCE(v.gross_revenue, 0)::numeric
    FROM date_series ds
    LEFT JOIN shopify.v_order_daily v 
        ON v.order_date = ds.order_date
        AND v.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
    ORDER BY ds.order_date;
    """
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (days, shop_domain))
            rows = await cur.fetchall()
            return [{"date": d, "revenue": float(r)} for d, r in rows]


@router.get("/customers/leaderboard")
async def customer_leaderboard(
    shop_domain: str = Depends(get_shop_from_token),
    limit: int = 50
):
    """
    Customer leaderboard with revenue, orders, profit, AOV, and last order date.
    
    Security: shop_domain extracted from validated session token.
    
    Profit is calculated using the same logic as cogs.py - matching COGS via variant_id.
    Filters out line items with NULL variant_id to ensure accurate COGS matching.
    """
    sql = """
    WITH customer_metrics AS (
        SELECT 
            c.customer_id,
            COALESCE(c.email, 'No Email') as customer_email,
            COALESCE(
                NULLIF(TRIM(c.first_name || ' ' || c.last_name), ''),
                c.email,
                'Guest Customer'
            ) as customer_name,
            COUNT(DISTINCT o.order_id)::int as total_orders,
            COALESCE(SUM(o.total_price), 0)::numeric as total_revenue,
            MAX(o.created_at) as last_order_date,
            -- Calculate profit using same logic as cogs.py
            COALESCE(
                SUM(li.quantity * li.price) - SUM(li.quantity * COALESCE(pv.cost, 0)),
                0
            )::numeric as total_profit,
            -- Count items with COGS data
            COUNT(DISTINCT CASE WHEN pv.cost IS NOT NULL THEN li.line_number END)::int as items_with_cogs,
            COUNT(DISTINCT CASE WHEN li.variant_id IS NOT NULL THEN li.line_number END)::int as total_items
        FROM shopify.customers c
        LEFT JOIN shopify.orders o ON c.customer_id = o.customer_id AND c.shop_id = o.shop_id
        LEFT JOIN shopify.order_line_items li ON o.order_id = li.order_id AND o.shop_id = li.shop_id
        LEFT JOIN shopify.product_variants pv ON li.variant_id = pv.variant_id AND li.shop_id = pv.shop_id
        WHERE c.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
          AND LOWER(COALESCE(o.financial_status,'')) IN ('paid', 'partially_paid', '')
          AND (li.variant_id IS NOT NULL OR li.line_number IS NULL)
        GROUP BY c.customer_id, c.email, c.first_name, c.last_name
    )
    SELECT 
        customer_id,
        customer_name,
        customer_email,
        total_orders,
        total_revenue,
        total_profit,
        CASE 
            WHEN total_orders > 0 THEN ROUND((total_revenue / total_orders)::numeric, 2)
            ELSE 0
        END as avg_order_value,
        last_order_date,
        -- Indicate if profit data is complete, partial, or unavailable
        CASE 
            WHEN items_with_cogs = 0 THEN 'unavailable'
            WHEN items_with_cogs = total_items THEN 'complete'
            ELSE 'partial'
        END as profit_data_status,
        items_with_cogs,
        total_items
    FROM customer_metrics
    WHERE total_orders > 0  -- Only include customers who have placed orders
    ORDER BY total_revenue DESC
    LIMIT %s;
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, limit))
            rows = await cur.fetchall()
            
            if not rows:
                return {
                    "customers": [],
                    "summary": {
                        "total_customers": 0,
                        "total_revenue": 0.0,
                        "total_profit": 0.0,
                        "profit_data_available": False
                    }
                }
            
            customers = []
            total_revenue = 0.0
            total_profit = 0.0
            has_any_cogs = False
            
            for row in rows:
                customer_id, customer_name, customer_email, total_orders, revenue, profit, aov, last_order, profit_status, items_with_cogs, total_items = row
                
                total_revenue += float(revenue)
                total_profit += float(profit)
                
                if profit_status != 'unavailable':
                    has_any_cogs = True
                
                customers.append({
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "customer_email": customer_email,
                    "total_orders": total_orders,
                    "total_revenue": float(revenue),
                    "total_profit": float(profit) if profit_status != 'unavailable' else None,
                    "avg_order_value": float(aov),
                    "last_order_date": last_order.isoformat() if last_order else None,
                    "profit_data_status": profit_status,
                    "profit_coverage": f"{items_with_cogs}/{total_items} items" if profit_status != 'unavailable' else None
                })
            
            return {
                "customers": customers,
                "summary": {
                    "total_customers": len(customers),
                    "total_revenue": round(total_revenue, 2),
                    "total_profit": round(total_profit, 2) if has_any_cogs else None,
                    "profit_data_available": has_any_cogs,
                    "avg_revenue_per_customer": round(total_revenue / len(customers), 2) if customers else 0.0,
                    "avg_profit_per_customer": round(total_profit / len(customers), 2) if has_any_cogs and customers else None
                }
            }


@router.get("/charts")
async def get_charts(shop_domain: str = Depends(get_shop_from_token)) -> Dict[str, List[Dict[str, Any]]]:
    """
    Generate Plotly chart data for authenticated shop, and include export URLs.
    
    Security: shop_domain extracted from validated session token.
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
                        "export_url": "/charts/export/monthly_revenue"
                    })

            # Chart 2: Top 5 Products (% of Total Revenue) with "Other"
            total_rev_sql = """
            SELECT COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_rev
            FROM shopify.order_line_items li
            JOIN shopify.orders o ON li.order_id = o.order_id AND li.shop_id = o.shop_id
            WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
            """
            top5_sql = """
            SELECT 
                li.title AS product_name,
                COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_revenue
            FROM shopify.order_line_items li
            JOIN shopify.orders o ON li.order_id = o.order_id AND li.shop_id = o.shop_id
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
                        "export_url": "/charts/export/top_products_revenue"
                    })

            # Chart 3: Daily Orders (Line Chart) - Last 30 days with ALL dates
            daily_orders_sql = """
            WITH date_series AS (
                SELECT generate_series(
                    current_date - 30,
                    current_date,
                    '1 day'::interval
                )::date AS order_date
            )
            SELECT 
                ds.order_date,
                COALESCE(COUNT(o.order_id), 0)::int as order_count
            FROM date_series ds
            LEFT JOIN shopify.orders o 
                ON DATE(o.created_at) = ds.order_date
                AND o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            GROUP BY ds.order_date
            ORDER BY ds.order_date;
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
                        "export_url": "/charts/export/daily_orders_30d"
                    })

            # Chart 4: Revenue by Day with ALL dates
            revenue_sql = """
            WITH date_series AS (
                SELECT generate_series(
                    current_date - 30,
                    current_date,
                    '1 day'::interval
                )::date AS order_date
            )
            SELECT 
                ds.order_date::text,
                COALESCE(v.gross_revenue, 0)::numeric
            FROM date_series ds
            LEFT JOIN shopify.v_order_daily v 
                ON v.order_date = ds.order_date
                AND v.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            ORDER BY ds.order_date;
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
                        "export_url": "/charts/export/daily_revenue_30d"
                    })

            # Chart 5: Top Customers by Revenue
            top_customers_sql = """
            WITH customer_metrics AS (
                SELECT 
                    c.customer_id,
                    COALESCE(
                        NULLIF(TRIM(c.first_name || ' ' || c.last_name), ''),
                        c.email,
                        'Guest Customer'
                    ) as customer_name,
                    COALESCE(SUM(o.total_price), 0)::numeric as total_revenue
                FROM shopify.customers c
                LEFT JOIN shopify.orders o ON c.customer_id = o.customer_id AND c.shop_id = o.shop_id
                WHERE c.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                  AND LOWER(COALESCE(o.financial_status,'')) IN ('paid', 'partially_paid', '')
                GROUP BY c.customer_id, c.email, c.first_name, c.last_name
                HAVING COUNT(DISTINCT o.order_id) > 0
            )
            SELECT customer_name, total_revenue
            FROM customer_metrics
            ORDER BY total_revenue DESC
            LIMIT 10;
            """
            async with conn.cursor() as cur:
                await cur.execute(top_customers_sql, (shop_domain,))
                customer_data = await cur.fetchall()

                if customer_data:
                    charts.append({
                        "key": "top_customers",
                        "data": [{
                            "x": [row[0] for row in customer_data],
                            "y": [float(row[1]) for row in customer_data],
                            "type": "bar",
                            "name": "Revenue",
                            "marker": {"color": "#008060"}
                        }],
                        "layout": {
                            "title": "Top 10 Customers by Revenue",
                            "xaxis": {"title": "Customer"},
                            "yaxis": {"title": "Total Revenue ($)"}
                        },
                        "export_url": "/charts/export/top_customers"
                    })

        if not charts:
            raise HTTPException(status_code=404, detail="No data found for this shop")

        return {"charts": charts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")


@router.get("/charts/export/{chart_key}")
async def export_chart_excel(
    chart_key: str,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Export the underlying dataset for a chart as an Excel file.
    
    Security: shop_domain extracted from validated session token.
    
    Valid chart_key values:
      - monthly_revenue
      - top_products_revenue
      - daily_orders_30d
      - daily_revenue_30d
      - top_customers
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
                JOIN shopify.orders o ON li.order_id = o.order_id AND li.shop_id = o.shop_id
                WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
                """
                top5_sql = """
                SELECT li.title AS product_name, COALESCE(SUM(li.quantity * li.price), 0)::numeric AS total_revenue
                FROM shopify.order_line_items li
                JOIN shopify.orders o ON li.order_id = o.order_id AND li.shop_id = o.shop_id
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
                WITH date_series AS (
                    SELECT generate_series(
                        current_date - 30,
                        current_date,
                        '1 day'::interval
                    )::date AS order_date
                )
                SELECT 
                    ds.order_date,
                    COALESCE(COUNT(o.order_id), 0)::int as order_count
                FROM date_series ds
                LEFT JOIN shopify.orders o 
                    ON DATE(o.created_at) = ds.order_date
                    AND o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                GROUP BY ds.order_date
                ORDER BY ds.order_date;
                """
                async with conn.cursor() as cur:
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                df = pd.DataFrame(rows, columns=["order_date", "order_count"])

            elif chart_key == "daily_revenue_30d":
                sql = """
                WITH date_series AS (
                    SELECT generate_series(
                        current_date - 30,
                        current_date,
                        '1 day'::interval
                    )::date AS order_date
                )
                SELECT 
                    ds.order_date,
                    COALESCE(v.gross_revenue, 0)::numeric
                FROM date_series ds
                LEFT JOIN shopify.v_order_daily v 
                    ON v.order_date = ds.order_date
                    AND v.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                ORDER BY ds.order_date;
                """
                async with conn.cursor() as cur:
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                df = pd.DataFrame(rows, columns=["order_date", "revenue"])

            elif chart_key == "top_customers":
                sql = """
                WITH customer_metrics AS (
                    SELECT 
                        c.customer_id,
                        COALESCE(c.email, 'No Email') as customer_email,
                        COALESCE(
                            NULLIF(TRIM(c.first_name || ' ' || c.last_name), ''),
                            c.email,
                            'Guest Customer'
                        ) as customer_name,
                        COUNT(DISTINCT o.order_id)::int as total_orders,
                        COALESCE(SUM(o.total_price), 0)::numeric as total_revenue,
                        MAX(o.created_at) as last_order_date,
                        -- Calculate profit using same logic as cogs.py
                        COALESCE(
                            SUM(li.quantity * li.price) - SUM(li.quantity * COALESCE(pv.cost, 0)),
                            0
                        )::numeric as total_profit,
                        COUNT(DISTINCT CASE WHEN pv.cost IS NOT NULL THEN li.line_number END)::int as items_with_cogs,
                        COUNT(DISTINCT CASE WHEN li.variant_id IS NOT NULL THEN li.line_number END)::int as total_items
                    FROM shopify.customers c
                    LEFT JOIN shopify.orders o ON c.customer_id = o.customer_id AND c.shop_id = o.shop_id
                    LEFT JOIN shopify.order_line_items li ON o.order_id = li.order_id AND o.shop_id = li.shop_id
                    LEFT JOIN shopify.product_variants pv ON li.variant_id = pv.variant_id AND li.shop_id = pv.shop_id
                    WHERE c.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
                      AND LOWER(COALESCE(o.financial_status,'')) IN ('paid', 'partially_paid', '')
                      AND (li.variant_id IS NOT NULL OR li.line_number IS NULL)
                    GROUP BY c.customer_id, c.email, c.first_name, c.last_name
                )
                SELECT 
                    customer_name as "Customer Name",
                    customer_email as "Email",
                    total_orders as "Total Orders",
                    total_revenue as "Total Revenue",
                    total_profit as "Total Profit",
                    CASE 
                        WHEN total_orders > 0 THEN ROUND((total_revenue / total_orders)::numeric, 2)
                        ELSE 0
                    END as "Avg Order Value",
                    last_order_date as "Last Order Date",
                    CASE 
                        WHEN items_with_cogs = 0 THEN 'No COGS Data'
                        WHEN items_with_cogs = total_items THEN 'Complete'
                        ELSE 'Partial (' || items_with_cogs || '/' || total_items || ')'
                    END as "Profit Data Coverage"
                FROM customer_metrics
                WHERE total_orders > 0
                ORDER BY total_revenue DESC
                LIMIT 50;
                """
                async with conn.cursor() as cur:
                    await cur.execute(sql, (shop_domain,))
                    rows = await cur.fetchall()
                    
                    # Get column names from cursor description
                    columns = [desc[0] for desc in cur.description]
                    
                    # Create DataFrame
                    df = pd.DataFrame(rows, columns=columns)
                    
                    # Format currency columns
                    if 'Total Revenue' in df.columns:
                        df['Total Revenue'] = df['Total Revenue'].apply(lambda x: float(x) if x else 0.0)
                    if 'Total Profit' in df.columns:
                        df['Total Profit'] = df['Total Profit'].apply(lambda x: float(x) if x else 0.0)
                    if 'Avg Order Value' in df.columns:
                        df['Avg Order Value'] = df['Avg Order Value'].apply(lambda x: float(x) if x else 0.0)
                    # FIX: Convert timezone-aware datetime to timezone-naive
                    if 'Last Order Date' in df.columns:
                        df['Last Order Date'] = pd.to_datetime(df['Last Order Date']).dt.tz_localize(None)

            else:
                raise HTTPException(404, f"Unknown chart_key: {chart_key}")

        # Build Excel in-memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            if chart_key == "top_customers":
                df.to_excel(writer, index=False, sheet_name="Customer Leaderboard")
                
                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets["Customer Leaderboard"]
                
                # Add currency format
                currency_format = workbook.add_format({'num_format': '$#,##0.00'})
                
                # Apply formatting to currency columns
                worksheet.set_column('D:D', 15, currency_format)  # Total Revenue
                worksheet.set_column('E:E', 15, currency_format)  # Total Profit
                worksheet.set_column('F:F', 15, currency_format)  # Avg Order Value
                
                # Auto-fit other columns
                worksheet.set_column('A:A', 25)  # Customer Name
                worksheet.set_column('B:B', 30)  # Email
                worksheet.set_column('C:C', 12)  # Total Orders
                worksheet.set_column('G:G', 20)  # Last Order Date
                worksheet.set_column('H:H', 20)  # Profit Data Coverage
            else:
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