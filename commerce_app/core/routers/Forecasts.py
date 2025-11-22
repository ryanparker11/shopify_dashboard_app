# commerce_app/core/routers/forecasts.py
from fastapi import APIRouter, HTTPException, Depends
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import statistics
import math  # NEW

router = APIRouter()


def get_shop_from_token(payload: Dict[str, Any] = Depends(verify_shopify_session_token)) -> str:
    """
    Extract shop domain from validated session token payload.
    The 'dest' claim contains the shop URL like: https://store.myshopify.com
    """
    dest = payload.get("dest", "")
    if not dest:
        raise HTTPException(401, "Missing shop in token")

    shop_domain = dest.replace("https://", "").replace("http://", "")

    if not shop_domain.endswith(".myshopify.com"):
        raise HTTPException(401, "Invalid shop domain in token")

    return shop_domain


# ---------------------------
# Helpers (NEW)
# ---------------------------

def linear_regression_slope(values: List[float]) -> float:
    """
    Compute slope (per day) using simple OLS on x=[0..n-1], y=values.
    Returns 0 if not enough data or variance.
    """
    n = len(values)
    if n < 3:
        return 0.0

    x = list(range(n))
    mean_x = (n - 1) / 2.0
    mean_y = statistics.mean(values)

    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, values))
    den = sum((xi - mean_x) ** 2 for xi in x)

    return (num / den) if den > 0 else 0.0


def weekday_factors(dates: List[datetime.date], values: List[float]) -> Dict[int, float]:
    """
    Learn weekday seasonality from history.
    Returns factor per weekday: avg(weekday)/overall_avg.
    """
    if not values:
        return {i: 1.0 for i in range(7)}

    overall_avg = statistics.mean(values)
    if overall_avg <= 0:
        return {i: 1.0 for i in range(7)}

    buckets: Dict[int, List[float]] = {i: [] for i in range(7)}
    for d, v in zip(dates, values):
        buckets[d.weekday()].append(v)

    factors = {}
    for wd in range(7):
        wd_avg = statistics.mean(buckets[wd]) if buckets[wd] else overall_avg
        factors[wd] = wd_avg / overall_avg if overall_avg > 0 else 1.0

    return factors


def widening_uncertainty(std_dev: float, horizon: int, scale_days: int = 14) -> float:
    """
    Increase uncertainty with horizon.
    Simple widening: std * (1 + horizon/scale_days).
    """
    return std_dev * (1.0 + (horizon / float(scale_days)))


# ---------------------------
# Revenue forecast
# ---------------------------

@router.get("/forecasts/revenue")
async def forecast_revenue(
    days: int = 30,
    lookback_days: int = 90,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Revenue forecast using:
      - trailing moving average baseline
      - OLS linear trend
      - learned weekday seasonality
      - widening uncertainty bounds
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
        COALESCE(v.gross_revenue, 0)::numeric as revenue
    FROM date_series ds
    LEFT JOIN shopify.v_order_daily v 
        ON v.order_date = ds.order_date
        AND v.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
    ORDER BY ds.order_date;
    """

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (lookback_days, shop_domain))
            rows = await cur.fetchall()

            if not rows:
                return {
                    "historical": [],
                    "forecast": [],
                    "metrics": {
                        "avg_daily_revenue": 0,
                        "daily_trend": 0,
                        "std_deviation": 0,  # NEW
                        "historical_total_30d": 0,
                        "forecast_total": 0
                    }
                }

            historical_data = [{"date": d, "revenue": float(r)} for d, r in rows]
            revenues = [float(r) for _, r in rows]
            n = len(revenues)

            # Baseline moving average
            window = min(14, n) if n >= 3 else n
            moving_avg = statistics.mean(revenues[-window:]) if window > 0 else 0

            # CHANGED: true linear regression slope
            daily_trend = linear_regression_slope(revenues)

            # NEW: learned weekday seasonality factors
            hist_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d, _ in rows]
            wd_factors = weekday_factors(hist_dates, revenues)

            # NEW: std deviation on recent window (for bounds)
            recent_slice = revenues[-window:] if window > 0 else revenues
            std_dev = statistics.stdev(recent_slice) if len(recent_slice) > 1 else 0.0

            forecast = []
            base_date = datetime.strptime(rows[-1][0], "%Y-%m-%d")

            for i in range(1, days + 1):
                forecast_date = base_date + timedelta(days=i)

                core_value = max(0.0, moving_avg + (daily_trend * i))

                wd = forecast_date.weekday()
                seasonal_value = core_value * wd_factors.get(wd, 1.0)

                # NEW: widening bounds
                horizon_std = widening_uncertainty(std_dev, i)
                lower = max(0.0, seasonal_value - horizon_std)
                upper = seasonal_value + horizon_std

                forecast.append({
                    "date": forecast_date.strftime("%Y-%m-%d"),
                    "forecast_revenue": round(seasonal_value, 2),
                    "lower_bound": round(lower, 2),   # NEW
                    "upper_bound": round(upper, 2),   # NEW
                    "confidence": "high" if i <= 7 else "medium" if i <= 14 else "low"  # CHANGED
                })

            last_30_days_revenue = sum([d["revenue"] for d in historical_data[-30:]])

            return {
                "historical": historical_data[-30:],
                "forecast": forecast,
                "metrics": {
                    "avg_daily_revenue": round(moving_avg, 2),
                    "daily_trend": round(daily_trend, 4),  # CHANGED (more precise)
                    "std_deviation": round(std_dev, 2),    # NEW
                    "historical_total_30d": round(last_30_days_revenue, 2),
                    "forecast_total": round(sum(f["forecast_revenue"] for f in forecast), 2)
                }
            }


# ---------------------------
# Orders forecast
# ---------------------------

@router.get("/forecasts/orders")
async def forecast_orders(
    days: int = 30,
    lookback_days: int = 90,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Order volume forecast using:
      - trailing moving average baseline
      - OLS linear trend
      - learned weekday seasonality
      - widening uncertainty bounds
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
        ds.order_date,
        COALESCE(COUNT(o.order_id), 0)::int as order_count
    FROM date_series ds
    LEFT JOIN shopify.orders o 
        ON o.order_date = ds.order_date
        AND o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
        AND o.financial_status IN ('paid', 'PAID', 'authorized', 'partially_paid')
    GROUP BY ds.order_date
    ORDER BY ds.order_date;
    """

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (lookback_days, shop_domain))
            rows = await cur.fetchall()

            if not rows:
                return {
                    "historical": [],
                    "forecast": [],
                    "metrics": {
                        "avg_daily_orders": 0,
                        "daily_trend": 0,
                        "std_deviation": 0,
                        "historical_total_30d": 0,
                        "forecast_total": 0
                    }
                }

            historical_data = [{"date": str(d), "orders": c} for d, c in rows]
            order_counts = [c for _, c in rows]
            n = len(order_counts)

            window = min(14, n) if n >= 3 else n
            moving_avg = statistics.mean(order_counts[-window:]) if window > 0 else 0

            # CHANGED: true linear regression slope
            daily_trend = linear_regression_slope([float(x) for x in order_counts])

            # NEW: learned weekday seasonality
            hist_dates = [d for d, _ in rows]  # already dates
            wd_factors = weekday_factors(hist_dates, [float(x) for x in order_counts])

            # std dev for bounds
            recent_slice = order_counts[-window:] if window > 0 else order_counts
            std_dev = statistics.stdev(recent_slice) if len(recent_slice) > 1 else 0.0

            forecast = []
            base_date = rows[-1][0]

            for i in range(1, days + 1):
                forecast_date = base_date + timedelta(days=i)

                core_value = max(0.0, moving_avg + (daily_trend * i))
                wd = forecast_date.weekday()
                seasonal_value = core_value * wd_factors.get(wd, 1.0)

                forecast_orders_val = round(seasonal_value)

                # NEW: widening bounds
                horizon_std = widening_uncertainty(std_dev, i)
                lower = max(0, round(forecast_orders_val - horizon_std))
                upper = round(forecast_orders_val + horizon_std)

                forecast.append({
                    "date": forecast_date.strftime("%Y-%m-%d"),
                    "forecast_orders": int(forecast_orders_val),
                    "lower_bound": int(lower),
                    "upper_bound": int(upper),
                    "confidence": "high" if i <= 7 else "medium" if i <= 14 else "low"  # CHANGED
                })

            last_30_days_orders = sum([d["orders"] for d in historical_data[-30:]])

            return {
                "historical": historical_data[-30:],
                "forecast": forecast,
                "metrics": {
                    "avg_daily_orders": round(moving_avg, 2),
                    "daily_trend": round(daily_trend, 4),
                    "std_deviation": round(std_dev, 2),
                    "historical_total_30d": last_30_days_orders,
                    "forecast_total": sum(f["forecast_orders"] for f in forecast)
                }
            }


# ---------------------------
# Inventory depletion
# ---------------------------

@router.get("/forecasts/inventory-depletion")
async def forecast_inventory_depletion(
    product_id: int = None,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Forecast when inventory will run out based on blended sales velocity.
    Includes items with zero sales as "no_velocity".
    """
    sql = """
    WITH recent_sales AS (
        SELECT 
            oli.product_id,
            oli.variant_id,
            SUM(CASE WHEN o.order_date >= current_date - 30 THEN oli.quantity ELSE 0 END)::int as units_sold_30d,
            SUM(CASE WHEN o.order_date >= current_date - 60 THEN oli.quantity ELSE 0 END)::int as units_sold_60d,
            MAX(o.order_date) as last_sale_date
        FROM shopify.orders o
        JOIN shopify.order_line_items oli 
            ON oli.shop_id = o.shop_id 
            AND oli.order_id = o.order_id
        WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
          AND o.order_date >= current_date - 60
          AND o.financial_status IN ('paid', 'PAID', 'authorized', 'partially_paid')
          AND oli.variant_id IS NOT NULL
        GROUP BY oli.product_id, oli.variant_id
    ),
    inventory_levels AS (
        SELECT 
            pv.product_id,
            pv.variant_id,
            p.title as product_title,
            pv.title as variant_title,
            pv.sku,
            pv.inventory_quantity,
            pv.inventory_policy
        FROM shopify.product_variants pv
        JOIN shopify.products p ON p.shop_id = pv.shop_id AND p.product_id = pv.product_id
        WHERE pv.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
          AND pv.inventory_quantity >= 0
    )
    SELECT 
        i.product_id,
        i.variant_id,
        i.product_title,
        i.variant_title,
        i.sku,
        i.inventory_quantity,
        COALESCE(s.units_sold_30d, 0) as units_sold_30d,
        COALESCE(s.units_sold_60d, 0) as units_sold_60d,
        s.last_sale_date
    FROM inventory_levels i
    LEFT JOIN recent_sales s ON s.product_id = i.product_id AND s.variant_id = i.variant_id
    WHERE (%s::bigint IS NULL OR i.product_id = %s::bigint)
    ORDER BY i.inventory_quantity ASC;
    """

    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, shop_domain, product_id, product_id))
            rows = await cur.fetchall()

            if not rows:
                return {"message": "No inventory data available", "products": []}

            forecasts = []
            alpha = 0.6  # NEW: weight recent 30d more heavily

            for row in rows:
                (prod_id, var_id, prod_title, var_title, sku,
                 inventory_qty, units_30d, units_60d, last_sale) = row

                velocity_30d = units_30d / 30.0
                velocity_60d = units_60d / 60.0

                # NEW: blended EWMA-like velocity
                blended_velocity = alpha * velocity_30d + (1 - alpha) * velocity_60d

                if blended_velocity > 0:
                    days_until_stockout = inventory_qty / blended_velocity
                    stockout_date = (datetime.now() + timedelta(days=days_until_stockout)).strftime("%Y-%m-%d")

                    risk = (
                        "critical" if days_until_stockout <= 7 else
                        "high" if days_until_stockout <= 14 else
                        "medium" if days_until_stockout <= 30 else
                        "low"
                    )
                else:
                    # NEW: include zero-sales items
                    days_until_stockout = None
                    stockout_date = None
                    risk = "no_velocity"

                forecasts.append({
                    "product_id": prod_id,
                    "variant_id": var_id,
                    "product_title": prod_title,
                    "variant_title": var_title,
                    "sku": sku,
                    "current_inventory": inventory_qty,
                    "units_sold_30d": units_30d,
                    "units_sold_60d": units_60d,           # NEW
                    "daily_velocity": round(blended_velocity, 2),  # CHANGED
                    "days_until_stockout": round(days_until_stockout, 1) if days_until_stockout else None,
                    "projected_stockout_date": stockout_date,
                    "risk_level": risk,
                    "last_sale_date": last_sale.strftime("%Y-%m-%d") if last_sale else None  # NEW
                })

            return {
                "products": forecasts,
                "summary": {
                    "total_products_tracked": len(forecasts),
                    "critical_risk": len([f for f in forecasts if f["risk_level"] == "critical"]),
                    "high_risk": len([f for f in forecasts if f["risk_level"] == "high"]),
                    "medium_risk": len([f for f in forecasts if f["risk_level"] == "medium"]),
                    "no_velocity": len([f for f in forecasts if f["risk_level"] == "no_velocity"])  # NEW
                }
            }


# ---------------------------
# Customer Lifetime Value
# ---------------------------

@router.get("/forecasts/customer-lifetime-value")
async def forecast_customer_lifetime_value(
    segment: str = None,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    CLV prediction with:
      - segment-based predicted lifespan
      - capped monthly frequency
      - churn risk from recency
    """
    try:
        sql = """
        WITH customer_metrics AS (
            SELECT 
                c.customer_id,
                c.email,
                c.first_name,
                c.last_name,
                COUNT(DISTINCT o.order_id)::int as total_orders,
                COALESCE(SUM(o.total_price), 0)::numeric as total_spent,
                COALESCE(AVG(o.total_price), 0)::numeric as avg_order_value,
                MIN(o.order_date) as first_order_date,
                MAX(o.order_date) as last_order_date,
                (MAX(o.order_date) - MIN(o.order_date))::int as customer_lifespan_days
            FROM shopify.customers c
            LEFT JOIN shopify.orders o 
                ON o.shop_id = c.shop_id 
                AND o.customer_id = c.customer_id
                AND o.financial_status IN ('paid', 'PAID', 'authorized', 'partially_paid')
            WHERE c.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            GROUP BY c.customer_id, c.email, c.first_name, c.last_name
            HAVING COUNT(DISTINCT o.order_id) > 0
        ),
        customer_segments AS (
            SELECT 
                *,
                CASE 
                    WHEN total_orders = 1 THEN 'new'
                    WHEN total_spent > 1000 THEN 'vip'
                    ELSE 'returning'
                END as segment,
                CASE 
                    WHEN customer_lifespan_days > 0 
                    THEN total_orders::float / GREATEST(customer_lifespan_days::float / 30.0, 1.0)  -- CHANGED safer denom
                    ELSE 0 
                END as monthly_order_frequency
            FROM customer_metrics
        )
        SELECT 
            customer_id,
            email,
            first_name,
            last_name,
            segment,
            total_orders,
            total_spent,
            avg_order_value,
            first_order_date,
            last_order_date,
            customer_lifespan_days,
            monthly_order_frequency
        FROM customer_segments
        WHERE (%s::text IS NULL OR segment = %s::text)
        ORDER BY total_spent DESC
        LIMIT 1000;
        """

        async with get_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (shop_domain, segment, segment))
                rows = await cur.fetchall()

                if not rows:
                    return {
                        "customers": [],
                        "summary": {
                            "total_customers": 0,
                            "avg_customer_lifetime_value": 0,
                            "total_predicted_value": 0,
                            "high_churn_risk": 0,
                            "segment_breakdown": {}
                        }
                    }

                # NEW: segment-based predicted lifespans
                predicted_lifespans = {
                    "new": 6,
                    "returning": 18,
                    "vip": 36
                }
                freq_cap = 10.0  # NEW: cap orders/month to avoid blowups

                customers = []
                for row in rows:
                    (cust_id, email, first_name, last_name, seg, total_orders,
                     total_spent, aov, first_order, last_order, lifespan_days, monthly_freq) = row

                    predicted_lifespan_months = predicted_lifespans.get(seg, 18)

                    # NEW: safer frequency + cap
                    raw_freq = float(monthly_freq) if monthly_freq is not None else 0.0
                    if raw_freq <= 0:
                        raw_freq = 1.0
                    monthly_frequency = min(raw_freq, freq_cap)

                    predicted_clv = float(aov) * monthly_frequency * predicted_lifespan_months

                    days_since_last_order = (datetime.now().date() - last_order).days if last_order else 999
                    churn_risk = (
                        "high" if days_since_last_order > 90 else
                        "medium" if days_since_last_order > 60 else
                        "low"
                    )

                    customers.append({
                        "customer_id": cust_id,
                        "email": email or "unknown",
                        "name": f"{first_name or ''} {last_name or ''}".strip() or "Unknown",
                        "segment": seg,
                        "total_orders": total_orders,
                        "total_spent": float(total_spent),
                        "avg_order_value": float(aov),
                        "first_order_date": first_order.strftime("%Y-%m-%d") if first_order else None,
                        "last_order_date": last_order.strftime("%Y-%m-%d") if last_order else None,
                        "customer_lifespan_days": lifespan_days or 0,
                        "monthly_order_frequency": round(monthly_frequency, 2),
                        "predicted_lifespan_months": predicted_lifespan_months,  # NEW
                        "predicted_clv": round(predicted_clv, 2),
                        "churn_risk": churn_risk,
                        "days_since_last_order": days_since_last_order
                    })

                segment_summary = {}
                for seg_type in ['new', 'returning', 'vip']:
                    seg_customers = [c for c in customers if c['segment'] == seg_type]
                    if seg_customers:
                        segment_summary[seg_type] = {
                            "count": len(seg_customers),
                            "avg_clv": round(statistics.mean([c['predicted_clv'] for c in seg_customers]), 2),
                            "total_value": round(sum([c['total_spent'] for c in seg_customers]), 2)
                        }

                return {
                    "customers": customers[:100],
                    "summary": {
                        "total_customers": len(customers),
                        "avg_customer_lifetime_value": round(statistics.mean([c['predicted_clv'] for c in customers]), 2) if customers else 0,
                        "total_predicted_value": round(sum([c['predicted_clv'] for c in customers]), 2) if customers else 0,
                        "high_churn_risk": len([c for c in customers if c['churn_risk'] == 'high']),
                        "segment_breakdown": segment_summary
                    }
                }

    except Exception as e:
        print(f"Error in forecast_customer_lifetime_value: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "customers": [],
            "summary": {
                "total_customers": 0,
                "avg_customer_lifetime_value": 0,
                "total_predicted_value": 0,
                "high_churn_risk": 0,
                "segment_breakdown": {}
            }
        }
