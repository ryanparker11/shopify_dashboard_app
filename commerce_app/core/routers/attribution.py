# commerce_app/core/routers/attribution.py
from fastapi import APIRouter, HTTPException, Query, Depends
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import json
import urllib.parse as urlparse
from collections import defaultdict

router = APIRouter()


def get_shop_from_session(session: Dict[str, Any]) -> str:
    """
    Extract shop domain from decoded session token.
    
    Args:
        session: Decoded JWT payload from verify_shopify_session_token
    
    Returns:
        str: Shop domain (e.g., "store.myshopify.com")
    """
    # Extract from 'dest' field (standard Shopify session token format)
    dest = session.get("dest", "")
    if dest:
        return dest
    
    # Fallback: extract from 'iss' field (format: https://store.myshopify.com/admin)
    iss = session.get("iss", "")
    if iss:
        # Remove https:// and /admin
        shop = iss.replace("https://", "").replace("/admin", "")
        return shop
    
    raise HTTPException(
        status_code=401,
        detail="Unable to extract shop domain from session token"
    )


def parse_utm_from_landing_site(landing_site: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Extract UTM parameters from landing_site URL.
    
    Args:
        landing_site: Full URL with potential UTM parameters
        
    Returns:
        Dict with utm_source, utm_medium, utm_campaign, utm_content, utm_term
    """
    if not landing_site:
        return {
            "utm_source": None,
            "utm_medium": None, 
            "utm_campaign": None,
            "utm_content": None,
            "utm_term": None
        }
    
    try:
        parsed = urlparse.urlparse(landing_site)
        params = urlparse.parse_qs(parsed.query)
        
        return {
            "utm_source": params.get("utm_source", [None])[0],
            "utm_medium": params.get("utm_medium", [None])[0],
            "utm_campaign": params.get("utm_campaign", [None])[0],
            "utm_content": params.get("utm_content", [None])[0],
            "utm_term": params.get("utm_term", [None])[0],
        }
    except Exception:
        return {
            "utm_source": None,
            "utm_medium": None,
            "utm_campaign": None,
            "utm_content": None,
            "utm_term": None
        }


def normalize_channel(
    utm_source: Optional[str],
    utm_medium: Optional[str], 
    source_name: Optional[str],
    referring_site: Optional[str]
) -> str:
    """
    Normalize various attribution sources into clean channel names.
    
    Priority:
    1. utm_source (most explicit)
    2. source_name (Shopify's classification)
    3. referring_site (parse domain)
    4. "Direct" (fallback)
    """
    # Clean up inputs
    utm_source = (utm_source or "").lower().strip()
    utm_medium = (utm_medium or "").lower().strip()
    source_name = (source_name or "").lower().strip()
    referring_site = (referring_site or "").lower().strip()
    
    # Handle Shopify-specific source names first (they're very accurate)
    if source_name:
        if source_name == "shopify_draft_order":
            return "Draft Order"
        elif source_name in ["pos", "retail"]:
            return "Point of Sale"
        elif source_name == "web":
            # If it's "web" but has UTM, defer to UTM parsing below
            if not utm_source and not referring_site:
                return "Direct"
        elif source_name == "android" or source_name == "ios":
            return "Mobile App"
        elif source_name == "checkout":
            return "Direct"
        # For other source_names, continue to other checks
    
    # UTM source takes priority for marketing channels
    if utm_source:
        # Map common UTM sources to friendly names
        if "google" in utm_source:
            return "Google Ads" if "cpc" in utm_medium or "ppc" in utm_medium else "Google"
        elif "facebook" in utm_source or "fb" in utm_source:
            return "Facebook"
        elif "instagram" in utm_source or "ig" in utm_source:
            return "Instagram"
        elif "tiktok" in utm_source:
            return "TikTok"
        elif "email" in utm_source or "klaviyo" in utm_source or "mailchimp" in utm_source:
            return "Email"
        elif "twitter" in utm_source or "x.com" in utm_source:
            return "Twitter"
        elif "pinterest" in utm_source:
            return "Pinterest"
        elif "youtube" in utm_source:
            return "YouTube"
        elif "linkedin" in utm_source:
            return "LinkedIn"
        elif "snapchat" in utm_source:
            return "Snapchat"
        elif "reddit" in utm_source:
            return "Reddit"
        elif "shopify" in utm_source:
            return "Shopify Marketing"
        elif "sms" in utm_source:
            return "SMS"
        else:
            # Return cleaned up UTM source
            return utm_source.replace("_", " ").replace("-", " ").title()
    
    # Parse referring site domain
    if referring_site:
        try:
            domain = urlparse.urlparse(referring_site).netloc or referring_site
            domain = domain.replace("www.", "")
            
            if "google" in domain:
                return "Google Organic"
            elif "facebook" in domain:
                return "Facebook"
            elif "instagram" in domain:
                return "Instagram"
            elif "tiktok" in domain:
                return "TikTok"
            elif "youtube" in domain:
                return "YouTube"
            elif "twitter" in domain or "t.co" in domain:
                return "Twitter"
            elif "pinterest" in domain:
                return "Pinterest"
            elif "linkedin" in domain:
                return "LinkedIn"
            elif "reddit" in domain:
                return "Reddit"
            else:
                # Return domain as referral
                return f"Referral: {domain.title()}"
        except Exception:
            return "Referral"
    
    # Default to Direct
    return "Direct"


def get_customer_type(orders_count: Optional[int]) -> str:
    """Determine if customer is new or returning based on their order count."""
    if not orders_count or orders_count <= 1:
        return "New"
    return "Repeat"


@router.get("/attribution/overview")
async def attribution_overview(
    session: dict = Depends(verify_shopify_session_token),
    days: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    Get marketing attribution overview showing channel performance.
    
    Returns:
    - Channel breakdown (orders, revenue, AOV, new vs repeat customers)
    - Data derived from Shopify's built-in attribution fields
    """
    
    # Extract shop_domain from session token
    shop_domain = get_shop_from_session(session)
    
    # First verify shop exists and get shop_id
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Get orders with attribution data
            # Extract landing_site, source_name, referring_site from root level of raw_json
            await cur.execute(
                """
                SELECT 
                    o.order_id,
                    o.total_price,
                    o.created_at,
                    (o.raw_json->>'landing_site')::text as landing_site,
                    (o.raw_json->>'source_name')::text as source_name,
                    (o.raw_json->>'referring_site')::text as referring_site,
                    (o.raw_json->>'landing_site_ref')::text as landing_site_ref,
                    c.orders_count
                FROM shopify.orders o
                LEFT JOIN shopify.customers c ON o.shop_id = c.shop_id AND o.customer_id = c.customer_id
                WHERE o.shop_id = %s
                  AND o.created_at >= NOW() - INTERVAL '%s days'
                  AND o.financial_status IN ('paid', 'partially_paid')
                ORDER BY o.created_at DESC
                """,
                (shop_id, days)
            )
            
            orders = await cur.fetchall()
    
    # Process orders to build channel attribution
    channel_stats = defaultdict(lambda: {
        "orders": 0,
        "revenue": 0.0,
        "new_customers": 0,
        "repeat_customers": 0,
        "order_ids": set()  # For deduplication
    })
    
    for order in orders:
        order_id, total_price, created_at, landing_site, source_name, referring_site, landing_site_ref, orders_count = order
        
        # Parse UTM parameters
        utm_data = parse_utm_from_landing_site(landing_site)
        
        # Normalize to channel
        channel = normalize_channel(
            utm_data["utm_source"],
            utm_data["utm_medium"],
            source_name,
            referring_site
        )
        
        # Aggregate stats
        channel_stats[channel]["orders"] += 1
        channel_stats[channel]["revenue"] += float(total_price or 0)
        
        # Track customer type
        customer_type = get_customer_type(orders_count)
        if customer_type == "New":
            channel_stats[channel]["new_customers"] += 1
        else:
            channel_stats[channel]["repeat_customers"] += 1
    
    # Format response
    channels = []
    for channel, stats in channel_stats.items():
        aov = stats["revenue"] / stats["orders"] if stats["orders"] > 0 else 0
        
        channels.append({
            "channel": channel,
            "orders": stats["orders"],
            "revenue": round(stats["revenue"], 2),
            "aov": round(aov, 2),
            "new_customers": stats["new_customers"],
            "repeat_customers": stats["repeat_customers"]
        })
    
    # Sort by revenue descending
    channels.sort(key=lambda x: x["revenue"], reverse=True)
    
    return {
        "channels": channels,
        "date_range": {
            "start": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end": datetime.now().date().isoformat(),
            "days": days
        }
    }


@router.get("/attribution/campaigns")
async def attribution_campaigns(
    session: dict = Depends(verify_shopify_session_token),
    days: int = Query(default=30, ge=1, le=365, description="Number of days to analyze"),
    limit: int = Query(default=20, ge=1, le=100, description="Max campaigns to return")
):
    """
    Get campaign-level attribution breakdown.
    
    Shows performance by utm_campaign when available.
    """
    
    # Extract shop_domain from session token
    shop_domain = get_shop_from_session(session)
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Get orders with attribution data
            await cur.execute(
                """
                SELECT 
                    o.order_id,
                    o.total_price,
                    o.created_at,
                    (o.raw_json->>'landing_site')::text as landing_site,
                    (o.raw_json->>'source_name')::text as source_name
                FROM shopify.orders o
                WHERE o.shop_id = %s
                  AND o.created_at >= NOW() - INTERVAL '%s days'
                  AND o.financial_status IN ('paid', 'partially_paid')
                ORDER BY o.created_at DESC
                """,
                (shop_id, days)
            )
            
            orders = await cur.fetchall()
    
    # Process campaigns
    campaign_stats = defaultdict(lambda: {
        "orders": 0,
        "revenue": 0.0,
        "source": None,
        "medium": None
    })
    
    for order in orders:
        order_id, total_price, created_at, landing_site, source_name = order
        
        # Parse UTM parameters
        utm_data = parse_utm_from_landing_site(landing_site)
        
        campaign = utm_data.get("utm_campaign")
        
        # Only track if campaign exists
        if campaign:
            campaign_stats[campaign]["orders"] += 1
            campaign_stats[campaign]["revenue"] += float(total_price or 0)
            
            # Store source/medium for context
            if not campaign_stats[campaign]["source"]:
                campaign_stats[campaign]["source"] = utm_data.get("utm_source")
                campaign_stats[campaign]["medium"] = utm_data.get("utm_medium")
    
    # Format response
    campaigns = []
    for campaign_name, stats in campaign_stats.items():
        campaigns.append({
            "campaign": campaign_name,
            "source": stats["source"],
            "medium": stats["medium"],
            "orders": stats["orders"],
            "revenue": round(stats["revenue"], 2),
            "avg_order_value": round(stats["revenue"] / stats["orders"], 2) if stats["orders"] > 0 else 0
        })
    
    # Sort by revenue descending and limit
    campaigns.sort(key=lambda x: x["revenue"], reverse=True)
    campaigns = campaigns[:limit]
    
    return {
        "campaigns": campaigns,
        "total_campaigns": len(campaign_stats),
        "date_range": {
            "start": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end": datetime.now().date().isoformat(),
            "days": days
        }
    }


@router.get("/attribution/trend")
async def attribution_trend(
    session: dict = Depends(verify_shopify_session_token),
    days: int = Query(default=30, ge=1, le=90, description="Number of days to analyze"),
    group_by: str = Query(default="day", regex="^(day|week)$", description="Group by day or week")
):
    """
    Get attribution trend over time showing channel performance by date.
    
    Useful for visualizing channel growth/changes over time.
    """
    
    # Extract shop_domain from session token
    shop_domain = get_shop_from_session(session)
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Determine date truncation based on group_by
            date_trunc = "day" if group_by == "day" else "week"
            
            # Get orders with attribution data grouped by date
            await cur.execute(
                f"""
                SELECT 
                    DATE_TRUNC(%s, o.created_at) as period,
                    (o.raw_json->>'landing_site')::text as landing_site,
                    (o.raw_json->>'source_name')::text as source_name,
                    (o.raw_json->>'referring_site')::text as referring_site,
                    COUNT(*) as orders,
                    SUM(o.total_price) as revenue
                FROM shopify.orders o
                WHERE o.shop_id = %s
                  AND o.created_at >= NOW() - INTERVAL '%s days'
                  AND o.financial_status IN ('paid', 'partially_paid')
                GROUP BY period, landing_site, source_name, referring_site
                ORDER BY period ASC
                """,
                (date_trunc, shop_id, days)
            )
            
            rows = await cur.fetchall()
    
    # Process into time series by channel
    time_series = defaultdict(lambda: defaultdict(lambda: {"orders": 0, "revenue": 0.0}))
    
    for row in rows:
        period, landing_site, source_name, referring_site, orders, revenue = row
        
        # Parse UTM and normalize channel
        utm_data = parse_utm_from_landing_site(landing_site)
        channel = normalize_channel(
            utm_data["utm_source"],
            utm_data["utm_medium"],
            source_name,
            referring_site
        )
        
        # Format period as string
        period_str = period.strftime("%Y-%m-%d")
        
        time_series[channel][period_str]["orders"] += orders
        time_series[channel][period_str]["revenue"] += float(revenue or 0)
    
    # Format for frontend (array of series)
    series = []
    for channel, periods in time_series.items():
        data_points = [
            {
                "date": period,
                "orders": stats["orders"],
                "revenue": round(stats["revenue"], 2)
            }
            for period, stats in sorted(periods.items())
        ]
        
        series.append({
            "channel": channel,
            "data": data_points
        })
    
    # Sort series by total revenue
    series.sort(key=lambda x: sum(d["revenue"] for d in x["data"]), reverse=True)
    
    return {
        "series": series,
        "group_by": group_by,
        "date_range": {
            "start": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end": datetime.now().date().isoformat(),
            "days": days
        }
    }


@router.get("/attribution/customer-split")
async def attribution_customer_split(
    session: dict = Depends(verify_shopify_session_token),
    days: int = Query(default=30, ge=1, le=365, description="Number of days to analyze")
):
    """
    Get new vs repeat customer breakdown by channel.
    
    Helps understand which channels drive new customer acquisition vs retention.
    """
    
    # Extract shop_domain from session token
    shop_domain = get_shop_from_session(session)
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Get orders with customer data
            await cur.execute(
                """
                SELECT 
                    (o.raw_json->>'landing_site')::text as landing_site,
                    (o.raw_json->>'source_name')::text as source_name,
                    (o.raw_json->>'referring_site')::text as referring_site,
                    c.orders_count,
                    o.total_price
                FROM shopify.orders o
                LEFT JOIN shopify.customers c ON o.shop_id = c.shop_id AND o.customer_id = c.customer_id
                WHERE o.shop_id = %s
                  AND o.created_at >= NOW() - INTERVAL '%s days'
                  AND o.financial_status IN ('paid', 'partially_paid')
                """,
                (shop_id, days)
            )
            
            orders = await cur.fetchall()
    
    # Process by channel and customer type
    channel_split = defaultdict(lambda: {
        "new": {"orders": 0, "revenue": 0.0},
        "repeat": {"orders": 0, "revenue": 0.0}
    })
    
    for order in orders:
        landing_site, source_name, referring_site, orders_count, total_price = order
        
        # Parse and normalize
        utm_data = parse_utm_from_landing_site(landing_site)
        channel = normalize_channel(
            utm_data["utm_source"],
            utm_data["utm_medium"],
            source_name,
            referring_site
        )
        
        customer_type = get_customer_type(orders_count)
        type_key = "new" if customer_type == "New" else "repeat"
        
        channel_split[channel][type_key]["orders"] += 1
        channel_split[channel][type_key]["revenue"] += float(total_price or 0)
    
    # Format response
    channels = []
    for channel, splits in channel_split.items():
        channels.append({
            "channel": channel,
            "new_customers": {
                "orders": splits["new"]["orders"],
                "revenue": round(splits["new"]["revenue"], 2),
                "percentage": round(
                    100 * splits["new"]["orders"] / (splits["new"]["orders"] + splits["repeat"]["orders"])
                    if (splits["new"]["orders"] + splits["repeat"]["orders"]) > 0 else 0,
                    1
                )
            },
            "repeat_customers": {
                "orders": splits["repeat"]["orders"],
                "revenue": round(splits["repeat"]["revenue"], 2),
                "percentage": round(
                    100 * splits["repeat"]["orders"] / (splits["new"]["orders"] + splits["repeat"]["orders"])
                    if (splits["new"]["orders"] + splits["repeat"]["orders"]) > 0 else 0,
                    1
                )
            }
        })
    
    # Sort by total orders
    channels.sort(
        key=lambda x: x["new_customers"]["orders"] + x["repeat_customers"]["orders"],
        reverse=True
    )
    
    return {
        "channels": channels,
        "date_range": {
            "start": (datetime.now() - timedelta(days=days)).date().isoformat(),
            "end": datetime.now().date().isoformat(),
            "days": days
        }
    }