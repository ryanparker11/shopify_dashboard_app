from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from typing import List, Dict, Any, Optional
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


# ------------- Formatting & Insight Helpers ------------- #

def format_currency(value: float) -> str:
    """Format a number as a short currency string, e.g. $1.2K, $3.4M."""
    if value is None:
        return "$0"
    abs_val = abs(value)
    if abs_val >= 1_000_000_000:
        return f"${value/1_000_000_000:.1f}B"
    if abs_val >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"${value/1_000:.1f}K"
    return f"${value:,.2f}"


def format_number(value: float) -> str:
    """Format large integers in a compact way (e.g. 1.2K)."""
    if value is None:
        return "0"
    abs_val = abs(value)
    if abs_val >= 1_000_000_000:
        return f"{value/1_000_000_000:.1f}B"
    if abs_val >= 1_000_000:
        return f"{value/1_000_000:.1f}M"
    if abs_val >= 1_000:
        return f"{value/1_000:.1f}K"
    return f"{int(value):d}"


def compute_delta(current: float, previous: float) -> Dict[str, Optional[float]]:
    """Compute absolute and percent deltas, guarding against divide-by-zero."""
    if previous is None:
        previous = 0.0
    if current is None:
        current = 0.0

    delta_amount = current - previous
    if previous == 0:
        delta_percent = None
    else:
        delta_percent = (delta_amount / previous) * 100.0

    if delta_amount > 0:
        direction = "up"
    elif delta_amount < 0:
        direction = "down"
    else:
        direction = "flat"

    return {
        "current": current,
        "previous": previous,
        "delta_amount": delta_amount,
        "delta_percent": delta_percent,
        "direction": direction,
    }


def build_alert_from_delta(
    delta: Dict[str, Optional[float]],
    metric_label: str,
    positive_good: bool = True,
    warn_threshold: float = 20.0
) -> Optional[Dict[str, Any]]:
    """
    Create a simple alert banner object from a delta result.
    - positive_good=True: declines trigger warnings, big gains trigger positive alerts.
    """
    delta_pct = delta.get("delta_percent")
    if delta_pct is None:
        return None

    direction = delta.get("direction")
    current_fmt = format_currency(delta["current"]) if "revenue" in metric_label.lower() else format_number(delta["current"])
    previous_fmt = format_currency(delta["previous"]) if "revenue" in metric_label.lower() else format_number(delta["previous"])

    # Determine severity based on sign and expectation
    if positive_good:
        if delta_pct <= -warn_threshold:
            return {
                "level": "warning",
                "metric": metric_label,
                "message": f"{metric_label} is down {abs(delta_pct):.1f}% "
                           f"({current_fmt} vs {previous_fmt})."
            }
        if delta_pct >= warn_threshold:
            return {
                "level": "positive",
                "metric": metric_label,
                "message": f"{metric_label} is up {delta_pct:.1f}% "
                           f"({current_fmt} vs {previous_fmt})."
            }
    else:
        # For metrics where lower is better (not currently used, but ready)
        if delta_pct >= warn_threshold:
            return {
                "level": "warning",
                "metric": metric_label,
                "message": f"{metric_label} worsened by {delta_pct:.1f}%."
            }
        if delta_pct <= -warn_threshold:
            return {
                "level": "positive",
                "metric": metric_label,
                "message": f"{metric_label} improved by {abs(delta_pct):.1f}%."
            }

    return None


# ------------- Summary / Orders Overview ------------- #

@router.get("/orders/summary")
async def orders_summary(shop_domain: str = Depends(get_shop_from_token)):
    """
    Get orders summary for authenticated shop, plus basic trend & narrative.
    
    Security: shop_domain extracted from validated session token.
    """
    summary_sql = """
    SELECT
      COUNT(*)::int                         AS total_orders,
      COALESCE(SUM(total_price),0)::numeric AS total_revenue,
      ROUND(AVG(NULLIF(total_price,0))::numeric,2) AS avg_order_value
    FROM shopify.orders
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
    """

    trend_sql = """
    SELECT
      COALESCE(SUM(CASE 
          WHEN created_at >= current_date - INTERVAL '30 days' 
          THEN total_price END), 0)::numeric AS revenue_30d,
      COALESCE(SUM(CASE 
          WHEN created_at >= current_date - INTERVAL '60 days'
           AND created_at <  current_date - INTERVAL '30 days'
          THEN total_price END), 0)::numeric AS revenue_prev_30d,
      COALESCE(SUM(CASE 
          WHEN created_at >= current_date - INTERVAL '30 days' 
          THEN 1 END), 0)::int AS orders_30d,
      COALESCE(SUM(CASE 
          WHEN created_at >= current_date - INTERVAL '60 days'
           AND created_at <  current_date - INTERVAL '30 days'
          THEN 1 END), 0)::int AS orders_prev_30d
    FROM shopify.orders
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s);
    """

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # Lifetime summary
            await cur.execute(summary_sql, (shop_domain,))
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(404, "Shop not found")
            total_orders, total_revenue, aov = row

            total_orders = int(total_orders)
            total_revenue = float(total_revenue) if total_revenue is not None else 0.0
            aov = float(aov) if aov is not None else 0.0

            # 30-day trend vs previous 30 days
            await cur.execute(trend_sql, (shop_domain,))
            trow = await cur.fetchone()
            if trow:
                revenue_30d, revenue_prev_30d, orders_30d, orders_prev_30d = trow
                revenue_30d = float(revenue_30d or 0.0)
                revenue_prev_30d = float(revenue_prev_30d or 0.0)
                orders_30d = int(orders_30d or 0)
                orders_prev_30d = int(orders_prev_30d or 0)
            else:
                revenue_30d = revenue_prev_30d = 0.0
                orders_30d = orders_prev_30d = 0

    rev_delta = compute_delta(revenue_30d, revenue_prev_30d)
    ord_delta = compute_delta(float(orders_30d), float(orders_prev_30d))

    insights: List[str] = []
    if rev_delta["delta_percent"] is not None:
        direction_word = "up" if rev_delta["direction"] == "up" else "down" if rev_delta["direction"] == "down" else "flat"
        insights.append(
            f"Revenue in the last 30 days is {direction_word} "
            f"{abs(rev_delta['delta_percent']):.1f}% vs the previous 30 days "
            f"({format_currency(revenue_30d)} vs {format_currency(revenue_prev_30d)})."
        )
    else:
        if revenue_prev_30d == 0 and revenue_30d > 0:
            insights.append(
                f"You generated {format_currency(revenue_30d)} in the last 30 days "
                f"from a baseline of $0 in the previous 30 days."
            )

    if ord_delta["delta_percent"] is not None and orders_prev_30d > 0:
        direction_word = "up" if ord_delta["direction"] == "up" else "down" if ord_delta["direction"] == "down" else "flat"
        insights.append(
            f"Order volume is {direction_word} "
            f"{abs(ord_delta['delta_percent']):.1f}% over the same period "
            f"({format_number(orders_30d)} vs {format_number(orders_prev_30d)} orders)."
        )

    alerts: List[Dict[str, Any]] = []
    rev_alert = build_alert_from_delta(rev_delta, "Revenue (last 30 days)")
    if rev_alert:
        alerts.append(rev_alert)
    ord_alert = build_alert_from_delta(ord_delta, "Orders (last 30 days)")
    if ord_alert:
        alerts.append(ord_alert)

    return {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "avg_order_value": aov,
        # New: formatted strings for nicer display
        "formatted": {
            "total_revenue": format_currency(total_revenue),
            "avg_order_value": format_currency(aov),
            "orders_30d": format_number(orders_30d),
            "orders_prev_30d": format_number(orders_prev_30d),
            "revenue_30d": format_currency(revenue_30d),
            "revenue_prev_30d": format_currency(revenue_prev_30d),
        },
        # New: trend deltas for narrative / badges
        "trend": {
            "revenue_30d": rev_delta,
            "orders_30d": ord_delta,
        },
        # New: insight sentences for display under KPIs or charts
        "insights": insights,
        # New: simple alerts for banners
        "alerts": alerts,
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
                    "profit_coverage": f"{items_with_cogs}/{total_items} items" if profit_status != 'unavailable' else None,
                    "formatted": {
                        "total_revenue": format_currency(float(revenue)),
                        "total_profit": format_currency(float(profit)) if profit_status != 'unavailable' else None,
                        "avg_order_value": format_currency(float(aov)),
                    }
                })
            
            avg_rev_per_cust = round(total_revenue / len(customers), 2) if customers else 0.0
            avg_profit_per_cust = round(total_profit / len(customers), 2) if has_any_cogs and customers else None

            summary_insights: List[str] = []
            if customers:
                top_customer = customers[0]
                summary_insights.append(
                    f"Top customer {top_customer['customer_name']} has generated "
                    f"{format_currency(top_customer['total_revenue'])} in lifetime revenue."
                )
                summary_insights.append(
                    f"On average, each customer in this leaderboard has generated "
                    f"{format_currency(avg_rev_per_cust)} in revenue."
                )

            return {
                "customers": customers,
                "summary": {
                    "total_customers": len(customers),
                    "total_revenue": round(total_revenue, 2),
                    "total_profit": round(total_profit, 2) if has_any_cogs else None,
                    "profit_data_available": has_any_cogs,
                    "avg_revenue_per_customer": avg_rev_per_cust,
                    "avg_profit_per_customer": avg_profit_per_cust,
                    "formatted": {
                        "total_revenue": format_currency(total_revenue),
                        "total_profit": format_currency(total_profit) if has_any_cogs else None,
                        "avg_revenue_per_customer": format_currency(avg_rev_per_cust),
                        "avg_profit_per_customer": format_currency(avg_profit_per_cust) if avg_profit_per_cust is not None else None,
                    },
                    "insights": summary_insights
                }
            }


@router.get("/charts")
async def get_charts(shop_domain: str = Depends(get_shop_from_token)) -> Dict[str, List[Dict[str, Any]]]:
    """
    Generate Plotly chart data for authenticated shop, and include export URLs.
    Now also includes:
      - summary: metric deltas for each chart
      - insights: narrative strings
      - alerts: simple alert banners
      - comparison: previous-period data for some charts (for toggles)
    
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
                    months = [row[0] for row in monthly_data]
                    revenues = [float(row[1]) for row in monthly_data]

                    chart = {
                        "key": "monthly_revenue",
                        "data": [{
                            "x": months,
                            "y": revenues,
                            "type": "bar",
                            "name": "Monthly Revenue",
                            "marker": {"color": "#008060"}
                        }],
                        "layout": {
                            "title": "Revenue Over Time (Last 12 Months)",
                            "xaxis": {"title": "Month"},
                            "yaxis": {
                                "title": "Revenue ($)",
                                "tickprefix": "$",
                                "separatethousands": True
                            }
                        },
                        "export_url": "/charts/export/monthly_revenue"
                    }

                    # Trend: last month vs previous month
                    if len(revenues) >= 2:
                        current_rev = revenues[-1]
                        prev_rev = revenues[-2]
                        delta = compute_delta(current_rev, prev_rev)
                        alert = build_alert_from_delta(delta, "Monthly revenue")

                        insight = (
                            f"Revenue in {months[-1]} was {format_currency(current_rev)}, "
                            f"{'up' if delta['direction'] == 'up' else 'down' if delta['direction'] == 'down' else 'flat'}"
                        )
                        if delta["delta_percent"] is not None:
                            insight += f" {abs(delta['delta_percent']):.1f}% vs {months[-2]} ({format_currency(prev_rev)})."
                        else:
                            insight += f" compared to {months[-2]} ({format_currency(prev_rev)})."

                        chart["summary"] = {
                            "current_month": months[-1],
                            "previous_month": months[-2],
                            "delta": delta,
                            "formatted": {
                                "current_revenue": format_currency(current_rev),
                                "previous_revenue": format_currency(prev_rev),
                            }
                        }
                        chart["insights"] = [insight]
                        chart["alerts"] = [alert] if alert else []
                    else:
                        chart["summary"] = None
                        chart["insights"] = []
                        chart["alerts"] = []

                    charts.append(chart)

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

                    chart = {
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
                    }

                    # Insight: top product contribution
                    top_product_name = labels[0]
                    top_product_rev = values[0]
                    top_share_pct = (top_product_rev / total_rev * 100.0) if total_rev else 0.0

                    chart["summary"] = {
                        "total_revenue": total_rev,
                        "top_product": {
                            "name": top_product_name,
                            "revenue": top_product_rev,
                            "share_percent": top_share_pct
                        },
                        "formatted": {
                            "total_revenue": format_currency(total_rev),
                            "top_product_revenue": format_currency(top_product_rev),
                        }
                    }
                    chart["insights"] = [
                        f"{top_product_name} accounts for {top_share_pct:.1f}% "
                        f"of tracked product revenue."
                    ]
                    chart["alerts"] = []

                    charts.append(chart)

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

            prev_daily_orders_sql = """
            WITH date_series AS (
                SELECT generate_series(
                    current_date - 60,
                    current_date - 31,
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

                await cur.execute(prev_daily_orders_sql, (shop_domain,))
                prev_daily_data = await cur.fetchall()

                if daily_data:
                    x_current = [row[0].strftime('%Y-%m-%d') for row in daily_data]
                    y_current = [int(row[1]) for row in daily_data]

                    chart = {
                        "key": "daily_orders_30d",
                        "data": [{
                            "x": x_current,
                            "y": y_current,
                            "type": "scatter",
                            "mode": "lines+markers",
                            "name": "Last 30 Days",
                            "line": {"color": "#5C6AC4", "width": 2},
                            "marker": {"size": 6}
                        }],
                        "layout": {
                            "title": "Orders Over Time (Last 30 Days)",
                            "xaxis": {"title": "Date"},
                            "yaxis": {"title": "Number of Orders"}
                        },
                        "export_url": "/charts/export/daily_orders_30d"
                    }

                    # Comparison: previous 30 days (for toggles)
                    if prev_daily_data:
                        x_prev = [row[0].strftime('%Y-%m-%d') for row in prev_daily_data]
                        y_prev = [int(row[1]) for row in prev_daily_data]
                    else:
                        x_prev, y_prev = [], []

                    chart["comparison"] = {
                        "previous_30d": {
                            "label": "Previous 30 Days",
                            "x": x_prev,
                            "y": y_prev,
                        }
                    }

                    # Trend & insights (sum last 7 vs previous 7 days)
                    total_current_30d = sum(y_current)
                    total_prev_30d = sum(y_prev) if y_prev else 0
                    delta = compute_delta(float(total_current_30d), float(total_prev_30d))

                    # 7-day window
                    last_7 = y_current[-7:] if len(y_current) >= 7 else y_current
                    prev_7 = y_current[-14:-7] if len(y_current) >= 14 else []
                    last_7_sum = float(sum(last_7))
                    prev_7_sum = float(sum(prev_7)) if prev_7 else 0.0
                    delta_7 = compute_delta(last_7_sum, prev_7_sum)

                    insights = []
                    if delta["delta_percent"] is not None and total_prev_30d > 0:
                        direction_word = "up" if delta["direction"] == "up" else "down" if delta["direction"] == "down" else "flat"
                        insights.append(
                            f"Order volume over the last 30 days is {direction_word} "
                            f"{abs(delta['delta_percent']):.1f}% vs the previous 30 days "
                            f"({format_number(total_current_30d)} vs {format_number(total_prev_30d)} orders)."
                        )
                    if delta_7["delta_percent"] is not None and prev_7_sum > 0:
                        direction_word = "up" if delta_7["direction"] == "up" else "down" if delta_7["direction"] == "down" else "flat"
                        insights.append(
                            f"In the last 7 days, orders are {direction_word} "
                            f"{abs(delta_7['delta_percent']):.1f}% vs the prior 7 days."
                        )

                    alert = build_alert_from_delta(delta, "Orders (last 30 days)")

                    chart["summary"] = {
                        "total_orders_30d": total_current_30d,
                        "total_orders_prev_30d": total_prev_30d,
                        "delta_30d": delta,
                        "delta_7d": delta_7,
                        "formatted": {
                            "total_orders_30d": format_number(total_current_30d),
                            "total_orders_prev_30d": format_number(total_prev_30d),
                        }
                    }
                    chart["insights"] = insights
                    chart["alerts"] = [alert] if alert else []

                    charts.append(chart)

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

            prev_revenue_sql = """
            WITH date_series AS (
                SELECT generate_series(
                    current_date - 60,
                    current_date - 31,
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

                await cur.execute(prev_revenue_sql, (shop_domain,))
                prev_revenue_data = await cur.fetchall()

                if revenue_data:
                    x_current = [row[0] for row in revenue_data]
                    y_current = [float(row[1]) for row in revenue_data]

                    chart = {
                        "key": "daily_revenue_30d",
                        "data": [{
                            "x": x_current,
                            "y": y_current,
                            "type": "scatter",
                            "mode": "lines",
                            "name": "Last 30 Days",
                            "line": {"color": "#008060", "width": 2},
                            "fill": "tozeroy"
                        }],
                        "layout": {
                            "title": "Daily Revenue (Last 30 Days)",
                            "xaxis": {"title": "Date"},
                            "yaxis": {
                                "title": "Revenue ($)",
                                "tickprefix": "$",
                                "separatethousands": True
                            }
                        },
                        "export_url": "/charts/export/daily_revenue_30d"
                    }

                    if prev_revenue_data:
                        x_prev = [row[0] for row in prev_revenue_data]
                        y_prev = [float(row[1]) for row in prev_revenue_data]
                    else:
                        x_prev, y_prev = [], []

                    chart["comparison"] = {
                        "previous_30d": {
                            "label": "Previous 30 Days",
                            "x": x_prev,
                            "y": y_prev
                        }
                    }

                    total_current_30d_rev = float(sum(y_current))
                    total_prev_30d_rev = float(sum(y_prev)) if y_prev else 0.0
                    delta_rev = compute_delta(total_current_30d_rev, total_prev_30d_rev)

                    # 7-day windows for insight
                    last_7 = y_current[-7:] if len(y_current) >= 7 else y_current
                    prev_7 = y_current[-14:-7] if len(y_current) >= 14 else []
                    last_7_sum = float(sum(last_7))
                    prev_7_sum = float(sum(prev_7)) if prev_7 else 0.0
                    delta_7_rev = compute_delta(last_7_sum, prev_7_sum)

                    insights = []
                    if delta_rev["delta_percent"] is not None and total_prev_30d_rev > 0:
                        direction_word = "up" if delta_rev["direction"] == "up" else "down" if delta_rev["direction"] == "down" else "flat"
                        insights.append(
                            f"Revenue over the last 30 days is {direction_word} "
                            f"{abs(delta_rev['delta_percent']):.1f}% vs the previous 30 days "
                            f"({format_currency(total_current_30d_rev)} vs {format_currency(total_prev_30d_rev)})."
                        )
                    if delta_7_rev["delta_percent"] is not None and prev_7_sum > 0:
                        direction_word = "up" if delta_7_rev["direction"] == "up" else "down" if delta_7_rev["direction"] == "down" else "flat"
                        insights.append(
                            f"In the last 7 days, revenue is {direction_word} "
                            f"{abs(delta_7_rev['delta_percent']):.1f}% vs the prior 7 days."
                        )

                    alert = build_alert_from_delta(delta_rev, "Revenue (last 30 days)")

                    chart["summary"] = {
                        "revenue_30d": total_current_30d_rev,
                        "revenue_prev_30d": total_prev_30d_rev,
                        "delta_30d": delta_rev,
                        "delta_7d": delta_7_rev,
                        "formatted": {
                            "revenue_30d": format_currency(total_current_30d_rev),
                            "revenue_prev_30d": format_currency(total_prev_30d_rev),
                        }
                    }
                    chart["insights"] = insights
                    chart["alerts"] = [alert] if alert else []

                    charts.append(chart)

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
                    names = [row[0] for row in customer_data]
                    revenues = [float(row[1]) for row in customer_data]
                    total_top_rev = float(sum(revenues))

                    chart = {
                        "key": "top_customers",
                        "data": [{
                            "x": names,
                            "y": revenues,
                            "type": "bar",
                            "name": "Revenue",
                            "marker": {"color": "#008060"}
                        }],
                        "layout": {
                            "title": "Top 10 Customers by Revenue",
                            "xaxis": {"title": "Customer"},
                            "yaxis": {
                                "title": "Total Revenue ($)",
                                "tickprefix": "$",
                                "separatethousands": True
                            }
                        },
                        "export_url": "/charts/export/top_customers"
                    }

                    top_name = names[0]
                    top_rev = revenues[0]
                    share_pct = (top_rev / total_top_rev * 100.0) if total_top_rev else 0.0

                    chart["summary"] = {
                        "total_top_revenue": total_top_rev,
                        "top_customer": {
                            "name": top_name,
                            "revenue": top_rev,
                            "share_percent": share_pct
                        },
                        "formatted": {
                            "total_top_revenue": format_currency(total_top_rev),
                            "top_customer_revenue": format_currency(top_rev),
                        }
                    }
                    chart["insights"] = [
                        f"{top_name} is your highest value customer in this list, "
                        f"with {format_currency(top_rev)} in revenue "
                        f"({share_pct:.1f}% of the top 10 total)."
                    ]
                    chart["alerts"] = []

                    charts.append(chart)

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
        headers = {"Content-Disposition": f'attachment; filename=\"{filename}\"'}
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")
