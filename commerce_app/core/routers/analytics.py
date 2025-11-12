from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import StreamingResponse
from commerce_app.core.db import get_conn
from typing import List, Dict, Any, Optional
from io import BytesIO
from datetime import datetime
import pandas as pd
import jwt
import os
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Get Shopify API secret for JWT validation
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET")
SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")


async def validate_session_token_optional(authorization: Optional[str] = Header(None)) -> Optional[dict]:
    """
    Validate Shopify session token if present.
    Returns None if token is missing or invalid (allows request to continue).
    This is for development - session tokens may not work on dev stores.
    """
    if not authorization:
        logger.warning("No Authorization header - proceeding without session token validation")
        return None
    
    if not authorization.startswith("Bearer "):
        logger.warning("Invalid Authorization format - proceeding without validation")
        return None
    
    token = authorization.replace("Bearer ", "")
    
    try:
        payload = jwt.decode(
            token,
            SHOPIFY_API_SECRET,
            algorithms=["HS256"],
            audience=SHOPIFY_API_KEY
        )
        
        shop_domain = payload.get("dest")
        if shop_domain:
            shop_domain = shop_domain.replace("https://", "").replace("http://", "")
            logger.info(f"✅ Valid session token for shop: {shop_domain}")
            return {
                "shop": shop_domain,
                "user_id": payload.get("sub"),
                "exp": payload.get("exp"),
            }
        else:
            logger.warning("Token missing 'dest' claim - proceeding without validation")
            return None
        
    except jwt.ExpiredSignatureError:
        logger.warning("Session token expired - proceeding without validation")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid session token: {e} - proceeding without validation")
        return None


@router.get("/orders/summary")
async def orders_summary(
    shop_domain: str,
    session: Optional[dict] = Header(None, alias="Authorization", include_in_schema=False)
):
    """
    Get order summary statistics for a shop.
    Validates session token if provided (for Shopify app store compliance).
    """
    # Try to validate session token
    validated = await validate_session_token_optional(session)
    
    # If token was validated, verify shop_domain matches
    if validated and validated["shop"] != shop_domain:
        logger.error(f"Shop mismatch: token={validated['shop']}, param={shop_domain}")
        raise HTTPException(status_code=403, detail="Access denied: shop mismatch")
    
    logger.info(f"Orders summary requested for shop: {shop_domain}")
    
    sql = """
    SELECT
      COUNT(*)::int AS total_orders,
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
    shop_domain: str,
    days: int = 30,
    authorization: Optional[str] = Header(None)
):
    """Get daily revenue - validates session token if provided."""
    validated = await validate_session_token_optional(authorization)
    if validated and validated["shop"] != shop_domain:
        raise HTTPException(status_code=403, detail="Access denied: shop mismatch")
    
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
async def get_charts(
    shop_domain: str,
    authorization: Optional[str] = Header(None)
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Generate Plotly chart data for a specific shop.
    Validates session token if provided (for Shopify app store compliance).
    """
    # Validate session token if provided
    validated = await validate_session_token_optional(authorization)
    if validated and validated["shop"] != shop_domain:
        raise HTTPException(status_code=403, detail="Access denied: shop mismatch")
    
    logger.info(f"Charts requested for shop: {shop_domain}")
    
    try:
        charts: List[Dict[str, Any]] = []

        async with get_conn() as conn:
            # Chart 1: Monthly Revenue
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

            # Add all your other charts here...
            # (Chart 2, 3, 4 from your original code)

        if not charts:
            raise HTTPException(status_code=404, detail="No data found for this shop")

        logger.info(f"Successfully generated {len(charts)} charts for {shop_domain}")
        return {"charts": charts}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating charts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error generating charts: {str(e)}")


@router.get("/charts/{shop_domain}/export/{chart_key}")
async def export_chart_excel(
    shop_domain: str,
    chart_key: str,
    authorization: Optional[str] = Header(None)
):
    """Export chart data - validates session token if provided."""
    validated = await validate_session_token_optional(authorization)
    if validated and validated["shop"] != shop_domain:
        raise HTTPException(status_code=403, detail="Access denied: shop mismatch")
    
    # Your existing export logic here...
    pass