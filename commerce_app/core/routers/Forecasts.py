from fastapi import APIRouter, HTTPException, Depends
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from datetime import datetime, timedelta
from typing import List, Dict, Any
import statistics

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


@router.get("/forecasts/revenue")
async def forecast_revenue(
    days: int = 30,
    lookback_days: int = 90,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Revenue forecast using moving average and trend analysis.
    
    Security: shop_domain extracted from validated session token, NOT query params.
    
    Args:
        shop_domain: Shop domain (from session token)
        days: Number of days to forecast forward
        lookback_days: Historical days to use for prediction (default 90)
    """
    sql = """
    SELECT 
        order_date::text,
        COALESCE(gross_revenue, 0)::numeric as revenue
    FROM shopify.v_order_daily
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
      AND order_date IS NOT NULL
      AND order_date >= current_date - %s::int
    ORDER BY order_date;
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, lookback_days))
            rows = await cur.fetchall()
            
            if not rows:
                # Return empty forecast instead of error
                return {
                    "historical": [],
                    "forecast": [],
                    "metrics": {
                        "avg_daily_revenue": 0,
                        "daily_trend": 0,
                        "forecast_total": 0
                    }
                }
            
            # Extract historical revenue
            historical_data = [{"date": d, "revenue": float(r)} for d, r in rows]
            revenues = [float(r) for _, r in rows]
            
            # Calculate trend using simple linear regression
            n = len(revenues)
            
            # Calculate moving average (use all available data if less than 14 days)
            window = min(14, n) if n >= 3 else n
            moving_avg = statistics.mean(revenues[-window:]) if window > 0 else 0
            
            # Calculate trend (needs at least 3 data points)
            if n >= 3:
                mid_point = max(1, n // 2)  # At least 1 for division
                older_avg = statistics.mean(revenues[:mid_point])
                recent_avg = statistics.mean(revenues[mid_point:])
                daily_trend = (recent_avg - older_avg) / mid_point if mid_point > 0 else 0
            else:
                # Not enough data for trend, just use average
                daily_trend = 0
            
            # Generate forecast
            forecast = []
            base_date = datetime.strptime(rows[-1][0], "%Y-%m-%d")
            
            for i in range(1, days + 1):
                forecast_date = base_date + timedelta(days=i)
                # Forecast = moving average + (trend * days ahead)
                forecast_value = max(0, moving_avg + (daily_trend * i))
                
                # Add day-of-week seasonality (simple version)
                day_of_week = forecast_date.weekday()
                # Weekend adjustment (typically lower for B2B, higher for B2C)
                weekend_factor = 0.85 if day_of_week >= 5 else 1.0
                
                forecast.append({
                    "date": forecast_date.strftime("%Y-%m-%d"),
                    "forecast_revenue": round(forecast_value * weekend_factor, 2),
                    "confidence": "medium" if i <= 14 else "low"
                })
            
            return {
                "historical": historical_data[-30:],  # Last 30 days
                "forecast": forecast,
                "metrics": {
                    "avg_daily_revenue": round(moving_avg, 2),
                    "daily_trend": round(daily_trend, 2),
                    "forecast_total": round(sum(f["forecast_revenue"] for f in forecast), 2)
                }
            }


@router.get("/forecasts/orders")
async def forecast_orders(
    days: int = 30,
    lookback_days: int = 90,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Order volume forecast using historical patterns.
    
    Security: shop_domain extracted from validated session token.
    
    Args:
        shop_domain: Shop domain (from session token)
        days: Number of days to forecast forward
        lookback_days: Historical days to use for prediction
    """
    sql = """
    SELECT 
        order_date,
        COUNT(*)::int as order_count
    FROM shopify.orders
    WHERE shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
      AND order_date IS NOT NULL
      AND order_date >= current_date - %s::int
      AND financial_status IN ('paid', 'PAID',  'authorized', 'partially_paid')
    GROUP BY order_date
    ORDER BY order_date;
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, lookback_days))
            rows = await cur.fetchall()
            
            if not rows:
                # Return empty forecast
                return {
                    "historical": [],
                    "forecast": [],
                    "metrics": {
                        "avg_daily_orders": 0,
                        "daily_trend": 0,
                        "std_deviation": 0,
                        "forecast_total": 0
                    }
                }
            
            historical_data = [{"date": str(d), "orders": c} for d, c in rows]
            order_counts = [c for _, c in rows]
            
            # Calculate statistics
            n = len(order_counts)
            
            # Use all available data if less than 14 days
            window = min(14, n) if n >= 3 else n
            moving_avg = statistics.mean(order_counts[-window:]) if window > 0 else 0
            std_dev = statistics.stdev(order_counts[-window:]) if len(order_counts[-window:]) > 1 else 0
            
            # Calculate trend (needs at least 3 data points)
            if n >= 3:
                mid_point = max(1, n // 2)
                older_avg = statistics.mean(order_counts[:mid_point])
                recent_avg = statistics.mean(order_counts[mid_point:])
                daily_trend = (recent_avg - older_avg) / mid_point if mid_point > 0 else 0
            else:
                daily_trend = 0
            
            # Generate forecast
            forecast = []
            base_date = rows[-1][0]
            
            for i in range(1, days + 1):
                forecast_date = base_date + timedelta(days=i)
                forecast_value = max(0, moving_avg + (daily_trend * i))
                
                # Day of week adjustment
                day_of_week = forecast_date.weekday()
                weekend_factor = 0.8 if day_of_week >= 5 else 1.0
                
                forecast_orders = round(forecast_value * weekend_factor)
                
                forecast.append({
                    "date": forecast_date.strftime("%Y-%m-%d"),
                    "forecast_orders": forecast_orders,
                    "lower_bound": max(0, round(forecast_orders - std_dev)),
                    "upper_bound": round(forecast_orders + std_dev),
                    "confidence": "high" if i <= 7 else "medium" if i <= 14 else "low"
                })
            
            return {
                "historical": historical_data[-30:],
                "forecast": forecast,
                "metrics": {
                    "avg_daily_orders": round(moving_avg, 2),
                    "daily_trend": round(daily_trend, 2),
                    "std_deviation": round(std_dev, 2),
                    "forecast_total": sum(f["forecast_orders"] for f in forecast)
                }
            }


@router.get("/forecasts/inventory-depletion")
async def forecast_inventory_depletion(
    product_id: int = None,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Forecast when inventory will run out based on sales velocity.
    
    Security: shop_domain extracted from validated session token.
    
    Args:
        shop_domain: Shop domain (from session token)
        product_id: Optional specific product ID to analyze
    """
    # Get product inventory and recent sales velocity
    sql = """
    WITH recent_sales AS (
        SELECT 
            pv.product_id,
            pv.variant_id,
            COUNT(DISTINCT o.order_id)::int as orders_count,
            SUM(
                (li.value->>'quantity')::int
            )::int as units_sold,
            MAX(o.order_date) as last_sale_date
        FROM shopify.orders o
        CROSS JOIN LATERAL jsonb_array_elements(o.line_items) li
        LEFT JOIN shopify.product_variants pv 
            ON pv.shop_id = o.shop_id 
            AND pv.variant_id = (li.value->>'variant_id')::bigint
        WHERE o.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
          AND o.order_date >= current_date - 30
          AND o.financial_status IN ('paid', 'PAID', 'authorized', 'partially_paid')
          AND pv.product_id IS NOT NULL
        GROUP BY pv.product_id, pv.variant_id
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
          AND pv.inventory_quantity > 0
          AND pv.inventory_policy = 'deny'  -- Only track items that stop selling when out of stock
    )
    SELECT 
        i.product_id,
        i.variant_id,
        i.product_title,
        i.variant_title,
        i.sku,
        i.inventory_quantity,
        COALESCE(s.units_sold, 0) as units_sold_30d,
        COALESCE(s.orders_count, 0) as orders_count,
        s.last_sale_date
    FROM inventory_levels i
    LEFT JOIN recent_sales s ON s.product_id = i.product_id AND s.variant_id = i.variant_id
    WHERE (%s::bigint IS NULL OR i.product_id = %s::bigint)
    ORDER BY 
        CASE 
            WHEN s.units_sold > 0 THEN i.inventory_quantity::float / (s.units_sold::float / 30.0)
            ELSE 999999 
        END;
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (shop_domain, shop_domain, product_id, product_id))
            rows = await cur.fetchall()
            
            if not rows:
                return {"message": "No inventory data available", "products": []}
            
            forecasts = []
            for row in rows:
                (prod_id, var_id, prod_title, var_title, sku, 
                 inventory_qty, units_sold, orders_count, last_sale) = row
                
                if units_sold > 0:
                    # Calculate daily sales velocity
                    daily_velocity = units_sold / 30.0
                    
                    # Days until stockout
                    days_until_stockout = inventory_qty / daily_velocity if daily_velocity > 0 else None
                    
                    # Projected stockout date
                    stockout_date = None
                    if days_until_stockout:
                        stockout_date = (datetime.now() + timedelta(days=days_until_stockout)).strftime("%Y-%m-%d")
                    
                    # Risk level
                    risk = "critical" if days_until_stockout and days_until_stockout <= 7 else \
                           "high" if days_until_stockout and days_until_stockout <= 14 else \
                           "medium" if days_until_stockout and days_until_stockout <= 30 else "low"
                    
                    forecasts.append({
                        "product_id": prod_id,
                        "variant_id": var_id,
                        "product_title": prod_title,
                        "variant_title": var_title,
                        "sku": sku,
                        "current_inventory": inventory_qty,
                        "units_sold_30d": units_sold,
                        "daily_velocity": round(daily_velocity, 2),
                        "days_until_stockout": round(days_until_stockout, 1) if days_until_stockout else None,
                        "projected_stockout_date": stockout_date,
                        "risk_level": risk
                    })
            
            return {
                "products": forecasts,
                "summary": {
                    "total_products_tracked": len(forecasts),
                    "critical_risk": len([f for f in forecasts if f["risk_level"] == "critical"]),
                    "high_risk": len([f for f in forecasts if f["risk_level"] == "high"]),
                    "medium_risk": len([f for f in forecasts if f["risk_level"] == "medium"])
                }
            }


@router.get("/forecasts/customer-lifetime-value")
async def forecast_customer_lifetime_value(
    segment: str = None,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Calculate Customer Lifetime Value (CLV) predictions.
    
    Security: shop_domain extracted from validated session token.
    
    Uses historical purchase behavior to predict future customer value.
    
    Args:
        shop_domain: Shop domain (from session token)
        segment: Optional customer segment filter ('new', 'returning', 'vip')
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
                EXTRACT(days FROM MAX(o.order_date) - MIN(o.order_date))::int as customer_lifespan_days,
                c.orders_count as shopify_order_count,
                c.total_spent as shopify_total_spent
            FROM shopify.customers c
            LEFT JOIN shopify.orders o 
                ON o.shop_id = c.shop_id 
                AND o.customer_id = c.customer_id
                AND o.financial_status IN ('paid', 'authorized', 'partially_paid')
            WHERE c.shop_id = (SELECT shop_id FROM shopify.shops WHERE shop_domain = %s)
            GROUP BY c.customer_id, c.email, c.first_name, c.last_name, c.orders_count, c.total_spent
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
                    THEN total_orders::float / (customer_lifespan_days::float / 30.0)
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
                
                customers = []
                for row in rows:
                    (cust_id, email, first_name, last_name, seg, total_orders, 
                     total_spent, aov, first_order, last_order, lifespan_days, monthly_freq) = row
                    
                    # Calculate CLV using simple formula: AOV × Purchase Frequency × Customer Lifespan
                    # Predicted lifespan: assume average customer stays active for 24 months
                    predicted_lifespan_months = 24
                    
                    # Monthly frequency based on historical behavior
                    monthly_frequency = monthly_freq if monthly_freq and monthly_freq > 0 else 1
                    
                    # Predicted CLV
                    predicted_clv = float(aov) * monthly_frequency * predicted_lifespan_months
                    
                    # Calculate churn risk based on days since last order
                    days_since_last_order = (datetime.now().date() - last_order).days if last_order else 999
                    churn_risk = "high" if days_since_last_order > 90 else \
                                "medium" if days_since_last_order > 60 else "low"
                    
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
                        "predicted_clv": round(predicted_clv, 2),
                        "churn_risk": churn_risk,
                        "days_since_last_order": days_since_last_order
                    })
                
                # Calculate segment summaries
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
                    "customers": customers[:100],  # Return top 100
                    "summary": {
                        "total_customers": len(customers),
                        "avg_customer_lifetime_value": round(statistics.mean([c['predicted_clv'] for c in customers]), 2) if customers else 0,
                        "total_predicted_value": round(sum([c['predicted_clv'] for c in customers]), 2) if customers else 0,
                        "high_churn_risk": len([c for c in customers if c['churn_risk'] == 'high']),
                        "segment_breakdown": segment_summary
                    }
                }
    except Exception as e:
        # Log the error and return empty data instead of 500
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