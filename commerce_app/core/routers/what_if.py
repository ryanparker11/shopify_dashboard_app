# commerce_app/core/routers/what_if.py
from fastapi import APIRouter, HTTPException, Query, Depends
from commerce_app.core.db import get_conn
from commerce_app.auth.session_tokens import verify_shopify_session_token
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import numpy as np
from collections import defaultdict

router = APIRouter()


def get_shop_from_token(payload: Dict[str, Any] = Depends(verify_shopify_session_token)) -> str:
    """
    Extract shop domain from validated session token payload.
    The 'dest' claim contains the shop URL like: https://store.myshopify.com
    """
    # Extract from 'dest' field (standard Shopify session token format)
    dest = payload.get("dest", "")
    if dest:
        # Remove https:// prefix to match database format
        shop_domain = dest.replace("https://", "").replace("http://", "")
        # Also remove any trailing paths like /admin
        shop_domain = shop_domain.split("/")[0]
        return shop_domain
    
    # Fallback: extract from 'iss' field (format: https://store.myshopify.com/admin)
    iss = payload.get("iss", "")
    if iss:
        # Remove https:// and /admin
        shop = iss.replace("https://", "").replace("/admin", "").split("/")[0]
        return shop
    
    raise HTTPException(
        status_code=401,
        detail="Unable to extract shop domain from session token"
    )


# ============================================
# Pydantic Models for Request/Response
# ============================================

class WhatIfVariables(BaseModel):
    """Variables that can be adjusted in what-if scenarios"""
    revenue_growth: float = Field(default=0.0, ge=-0.5, le=1.0, description="Revenue growth rate (-50% to +100%)")
    aov_change: float = Field(default=0.0, ge=-0.5, le=0.5, description="Average order value change (-50% to +50%)")
    order_volume_change: float = Field(default=0.0, ge=-0.5, le=1.0, description="Order volume change (-50% to +100%)")
    cogs_change: float = Field(default=0.0, ge=-0.3, le=0.5, description="COGS change (-30% to +50%)")
    conversion_rate_change: float = Field(default=0.0, ge=-0.3, le=0.5, description="Conversion rate change (-30% to +50%)")
    price_multiplier: float = Field(default=1.0, ge=0.5, le=2.0, description="Price multiplier (0.5x to 2.0x, where 1.0 = no change)")
    price_elasticity: float = Field(default=-1.5, ge=-3.0, le=0.0, description="Price elasticity of demand (typically -1.0 to -2.0)")


class SimulationRequest(BaseModel):
    """Request model for Monte Carlo simulation"""
    base_period_days: int = Field(default=90, ge=30, le=365, description="Historical days to analyze")
    forecast_days: int = Field(default=30, ge=7, le=180, description="Days to forecast")
    simulations: int = Field(default=10000, ge=1000, le=50000, description="Number of simulations to run")
    variables: WhatIfVariables = Field(default_factory=WhatIfVariables)


# ============================================
# Helper Functions
# ============================================

def calculate_statistics(data: np.ndarray) -> Dict[str, float]:
    """Calculate statistical metrics from simulation results"""
    return {
        "mean": float(np.mean(data)),
        "median": float(np.median(data)),
        "std_dev": float(np.std(data)),
        "min": float(np.min(data)),
        "max": float(np.max(data)),
        "percentile_5": float(np.percentile(data, 5)),
        "percentile_25": float(np.percentile(data, 25)),
        "percentile_75": float(np.percentile(data, 75)),
        "percentile_95": float(np.percentile(data, 95)),
        "confidence_90": [float(np.percentile(data, 5)), float(np.percentile(data, 95))],
        "confidence_95": [float(np.percentile(data, 2.5)), float(np.percentile(data, 97.5))],
    }


def create_histogram(data: np.ndarray, bins: int = 50) -> Dict[str, List]:
    """Create histogram data for visualization"""
    counts, bin_edges = np.histogram(data, bins=bins)
    
    return {
        "bins": [float(x) for x in bin_edges],
        "frequencies": [int(x) for x in counts],
        "bin_centers": [float((bin_edges[i] + bin_edges[i+1]) / 2) for i in range(len(bin_edges) - 1)]
    }


def calculate_price_elasticity_effect(price_change_pct: float, elasticity: float) -> float:
    """
    Calculate the demand change based on price elasticity.
    
    Price elasticity formula: % change in demand = elasticity * % change in price
    
    Example with elasticity of -1.5:
    - 10% price increase → 15% demand decrease
    - 20% price decrease → 30% demand increase
    
    Args:
        price_change_pct: Percentage change in price (e.g., 0.10 for 10% increase)
        elasticity: Price elasticity of demand (typically negative, e.g., -1.5)
    
    Returns:
        Percentage change in demand/order volume
    """
    return price_change_pct * elasticity


def calculate_sensitivity(base_results: np.ndarray, variables: Dict[str, float], 
                         historical_data: Dict[str, Any]) -> Dict[str, float]:
    """
    Calculate sensitivity of each variable (how much it impacts the outcome)
    Uses partial derivative approximation
    """
    sensitivity = {}
    
    # This is a simplified sensitivity - in practice you'd run multiple sims
    # For now, use coefficient of variation as a proxy
    base_variance = np.var(base_results)
    
    for var_name, var_value in variables.items():
        if var_value != 0:
            # Normalize by the variable's magnitude
            impact = abs(var_value) * base_variance
            sensitivity[var_name] = float(impact)
        else:
            sensitivity[var_name] = 0.0
    
    # Normalize to percentages
    total = sum(sensitivity.values())
    if total > 0:
        sensitivity = {k: (v / total) * 100 for k, v in sensitivity.items()}
    
    return sensitivity


# ============================================
# Main Endpoints
# ============================================

@router.get("/what-if/baseline")
async def get_baseline_metrics(
    days: int = Query(default=90, ge=30, le=365, description="Days of historical data"),
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Get baseline metrics from historical data to inform what-if scenarios.
    
    Returns:
    - Current average order value
    - Daily order volume
    - Revenue trends
    - COGS averages
    - Volatility measures
    """
    
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # Get shop_id
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Get daily metrics for baseline period
            await cur.execute(
                """
                SELECT 
                    o.order_date,
                    COUNT(DISTINCT o.order_id) as daily_orders,
                    COALESCE(SUM(o.total_price), 0) as daily_revenue,
                    COALESCE(AVG(o.total_price), 0) as avg_order_value,
                    COALESCE(SUM(oli.quantity * pv.cost), 0) as daily_cogs
                FROM shopify.orders o
                LEFT JOIN shopify.order_line_items oli 
                    ON o.shop_id = oli.shop_id AND o.order_id = oli.order_id
                LEFT JOIN shopify.product_variants pv 
                    ON oli.shop_id = pv.shop_id 
                    AND oli.product_id = pv.product_id 
                    AND oli.variant_id = pv.variant_id
                WHERE o.shop_id = %s
                  AND o.order_date >= CURRENT_DATE - %s
                  AND o.financial_status IN ('paid', 'PAID','partially_paid','PARTIALLY_PAID')
                GROUP BY o.order_date
                ORDER BY o.order_date DESC
                """,
                (shop_id, days)
            )
            
            daily_data = await cur.fetchall()
    
    if not daily_data:
        raise HTTPException(404, "No historical data found for baseline calculation")
    
    # Process daily data
    dates = []
    daily_orders = []
    daily_revenues = []
    daily_aovs = []
    daily_cogs = []
    
    for row in daily_data:
        order_date, orders, revenue, aov, cogs = row
        dates.append(order_date.isoformat())
        daily_orders.append(int(orders))
        daily_revenues.append(float(revenue))
        daily_aovs.append(float(aov))
        daily_cogs.append(float(cogs))
    
    # Calculate statistics
    total_revenue = sum(daily_revenues)
    total_orders = sum(daily_orders)
    total_cogs = sum(daily_cogs)
    
    avg_daily_revenue = np.mean(daily_revenues)
    avg_daily_orders = np.mean(daily_orders)
    avg_aov = np.mean(daily_aovs)
    avg_daily_cogs = np.mean(daily_cogs)
    
    # Calculate volatility (standard deviation)
    revenue_volatility = np.std(daily_revenues)
    order_volatility = np.std(daily_orders)
    aov_volatility = np.std(daily_aovs)
    
    # Calculate growth rate (simple linear trend)
    if len(daily_revenues) > 1:
        revenue_trend = (daily_revenues[0] - daily_revenues[-1]) / daily_revenues[-1] if daily_revenues[-1] > 0 else 0
    else:
        revenue_trend = 0
    
    # Calculate profit metrics
    total_profit = total_revenue - total_cogs
    profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    return {
        "period": {
            "days": days,
            "start_date": dates[-1] if dates else None,
            "end_date": dates[0] if dates else None
        },
        "totals": {
            "revenue": round(total_revenue, 2),
            "orders": total_orders,
            "cogs": round(total_cogs, 2),
            "profit": round(total_profit, 2),
            "profit_margin": round(profit_margin, 2)
        },
        "averages": {
            "daily_revenue": round(avg_daily_revenue, 2),
            "daily_orders": round(avg_daily_orders, 2),
            "order_value": round(avg_aov, 2),
            "daily_cogs": round(avg_daily_cogs, 2)
        },
        "volatility": {
            "revenue_std_dev": round(revenue_volatility, 2),
            "order_std_dev": round(order_volatility, 2),
            "aov_std_dev": round(aov_volatility, 2),
            "revenue_coefficient_of_variation": round((revenue_volatility / avg_daily_revenue * 100) if avg_daily_revenue > 0 else 0, 2)
        },
        "trends": {
            "revenue_growth_rate": round(revenue_trend * 100, 2)
        },
        "time_series": {
            "dates": dates[:30],  # Last 30 days for visualization
            "daily_revenue": daily_revenues[:30],
            "daily_orders": daily_orders[:30]
        }
    }


@router.post("/what-if/simulate")
async def run_monte_carlo_simulation(
    request: SimulationRequest,
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Run Monte Carlo simulation with what-if variables.
    
    This simulates thousands of possible futures based on:
    - Historical volatility
    - User-defined variable changes
    - Price elasticity effects (price changes affect order volume)
    - Random sampling from probability distributions
    
    Returns probability distributions and statistics for revenue, profit, etc.
    """
    
    # First get baseline metrics
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            # Get shop_id
            await cur.execute(
                "SELECT shop_id FROM shopify.shops WHERE shop_domain = %s",
                (shop_domain,)
            )
            shop_row = await cur.fetchone()
            if not shop_row:
                raise HTTPException(404, "Shop not found")
            
            shop_id = shop_row[0]
            
            # Get historical data
            await cur.execute(
                """
                SELECT 
                    o.order_date,
                    COUNT(DISTINCT o.order_id) as daily_orders,
                    COALESCE(SUM(o.total_price), 0) as daily_revenue,
                    COALESCE(AVG(o.total_price), 0) as avg_order_value,
                    COALESCE(SUM(oli.quantity * pv.cost), 0) as daily_cogs
                FROM shopify.orders o
                LEFT JOIN shopify.order_line_items oli 
                    ON o.shop_id = oli.shop_id AND o.order_id = oli.order_id
                LEFT JOIN shopify.product_variants pv 
                    ON oli.shop_id = pv.shop_id 
                    AND oli.product_id = pv.product_id 
                    AND oli.variant_id = pv.variant_id
                WHERE o.shop_id = %s
                  AND o.order_date >= CURRENT_DATE - %s
                  AND o.financial_status IN ('paid', 'PAID','partially_paid','PARTIALLY_PAID')
                GROUP BY o.order_date
                ORDER BY o.order_date DESC
                """,
                (shop_id, request.base_period_days)
            )
            
            historical_data = await cur.fetchall()
    
    if not historical_data:
        raise HTTPException(404, "No historical data found for simulation")
    
    # Extract historical metrics
    daily_revenues = np.array([float(row[2]) for row in historical_data])
    daily_orders = np.array([float(row[1]) for row in historical_data])
    daily_aovs = np.array([float(row[3]) for row in historical_data])
    daily_cogs = np.array([float(row[4]) for row in historical_data])
    
    # Calculate baseline statistics
    base_daily_revenue = np.mean(daily_revenues)
    base_daily_orders = np.mean(daily_orders)
    base_aov = np.mean(daily_aovs)
    base_daily_cogs = np.mean(daily_cogs)
    
    # Calculate volatility (standard deviation)
    revenue_std = np.std(daily_revenues)
    order_std = np.std(daily_orders)
    aov_std = np.std(daily_aovs)
    cogs_std = np.std(daily_cogs)
    
    # Run Monte Carlo simulation
    n_sims = request.simulations
    forecast_days = request.forecast_days
    
    # Initialize result arrays
    simulated_revenues = np.zeros(n_sims)
    simulated_orders = np.zeros(n_sims)
    simulated_profits = np.zeros(n_sims)
    simulated_margins = np.zeros(n_sims)
    
    # Set random seed for reproducibility (optional)
    np.random.seed(42)
    
    # Calculate price change percentage from multiplier (1.0 = no change, 1.1 = 10% increase)
    price_change_pct = request.variables.price_multiplier - 1.0
    
    # Calculate demand effect from price elasticity
    # e.g., 10% price increase with -1.5 elasticity = -15% demand
    elasticity_demand_effect = calculate_price_elasticity_effect(
        price_change_pct, 
        request.variables.price_elasticity
    )
    
    # Apply what-if adjustments to baseline
    # Price multiplier directly affects AOV (in addition to any manual aov_change)
    adjusted_aov = base_aov * request.variables.price_multiplier * (1 + request.variables.aov_change)
    
    # Order volume is affected by:
    # 1. Manual order_volume_change
    # 2. Price elasticity effect (price increase reduces demand)
    adjusted_daily_orders = base_daily_orders * (1 + request.variables.order_volume_change) * (1 + elasticity_demand_effect)
    
    # Revenue growth is applied on top of price/volume effects
    adjusted_daily_revenue = adjusted_aov * adjusted_daily_orders * (1 + request.variables.revenue_growth)
    
    # COGS rate adjustment (COGS as percentage of revenue)
    # Note: When prices increase, COGS stays same per unit, so margin improves
    base_cogs_per_order = base_daily_cogs / base_daily_orders if base_daily_orders > 0 else 0
    adjusted_cogs_per_order = base_cogs_per_order * (1 + request.variables.cogs_change)
    
    # Run simulations
    for i in range(n_sims):
        # Sample AOV with price-adjusted mean
        sim_aov = np.random.normal(adjusted_aov, aov_std * 0.8)
        sim_aov = max(sim_aov, adjusted_aov * 0.5)  # Floor at 50% of adjusted AOV
        
        # Sample daily orders with elasticity-adjusted mean
        daily_sim_orders = np.random.normal(
            adjusted_daily_orders,
            order_std * 0.8,
            forecast_days
        )
        daily_sim_orders = np.maximum(daily_sim_orders, 0)
        
        # Calculate daily revenue
        daily_sim_revenues = daily_sim_orders * sim_aov * (1 + request.variables.revenue_growth)
        daily_sim_revenues = np.maximum(daily_sim_revenues, 0)
        
        # Calculate totals for this simulation
        total_revenue = np.sum(daily_sim_revenues)
        total_orders = np.sum(daily_sim_orders)
        
        # Calculate COGS (per-order basis, not affected by price increase)
        total_cogs = total_orders * adjusted_cogs_per_order
        
        # Calculate profit
        total_profit = total_revenue - total_cogs
        profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        
        # Store results
        simulated_revenues[i] = total_revenue
        simulated_orders[i] = total_orders
        simulated_profits[i] = total_profit
        simulated_margins[i] = profit_margin
    
    # Calculate statistics for each metric
    revenue_stats = calculate_statistics(simulated_revenues)
    profit_stats = calculate_statistics(simulated_profits)
    order_stats = calculate_statistics(simulated_orders)
    margin_stats = calculate_statistics(simulated_margins)
    
    # Create histograms
    revenue_histogram = create_histogram(simulated_revenues)
    profit_histogram = create_histogram(simulated_profits)
    
    # Calculate probability of positive profit
    probability_positive_profit = float(np.sum(simulated_profits > 0) / n_sims)
    
    # Calculate sensitivity analysis (include price_multiplier effect)
    sensitivity = calculate_sensitivity(
        simulated_revenues,
        {
            "revenue_growth": request.variables.revenue_growth,
            "aov_change": request.variables.aov_change,
            "order_volume_change": request.variables.order_volume_change,
            "cogs_change": request.variables.cogs_change,
            "price_multiplier": price_change_pct,  # Use the percentage change
        },
        {"base_revenue": base_daily_revenue}
    )
    
    return {
        "simulation_id": f"sim_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "inputs": {
            "base_period_days": request.base_period_days,
            "forecast_days": request.forecast_days,
            "simulations": request.simulations,
            "variables": request.variables.dict()
        },
        "baseline": {
            "daily_revenue": round(base_daily_revenue, 2),
            "daily_orders": round(base_daily_orders, 2),
            "average_order_value": round(base_aov, 2),
            "cogs_rate": round((base_daily_cogs / base_daily_revenue * 100) if base_daily_revenue > 0 else 0, 2)
        },
        "price_analysis": {
            "price_change_percent": round(price_change_pct * 100, 2),
            "elasticity_used": request.variables.price_elasticity,
            "demand_effect_percent": round(elasticity_demand_effect * 100, 2),
            "adjusted_aov": round(adjusted_aov, 2),
            "adjusted_daily_orders": round(adjusted_daily_orders, 2)
        },
        "results": {
            "revenue": {
                **revenue_stats,
                "histogram": revenue_histogram
            },
            "profit": {
                **profit_stats,
                "histogram": profit_histogram,
                "probability_positive": round(probability_positive_profit * 100, 2)
            },
            "orders": order_stats,
            "profit_margin": margin_stats
        },
        "sensitivity": sensitivity,
        "insights": generate_insights(
            revenue_stats, 
            profit_stats, 
            probability_positive_profit,
            sensitivity,
            request.variables,
            price_change_pct,
            elasticity_demand_effect
        )
    }


@router.get("/what-if/price-elasticity-preview")
async def preview_price_elasticity(
    price_multiplier: float = Query(default=1.0, ge=0.5, le=2.0, description="Price multiplier (1.0 = no change)"),
    elasticity: float = Query(default=-1.5, ge=-3.0, le=0.0, description="Price elasticity of demand"),
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Preview the effect of a price change without running full simulation.
    
    Useful for the frontend slider to show real-time impact estimates.
    
    Returns:
    - Expected demand change
    - Estimated revenue impact
    - Break-even analysis
    """
    
    # Get current baseline
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
            
            # Get recent averages (last 30 days)
            await cur.execute(
                """
                SELECT 
                    COALESCE(AVG(daily_revenue), 0) as avg_revenue,
                    COALESCE(AVG(daily_orders), 0) as avg_orders,
                    COALESCE(AVG(daily_aov), 0) as avg_aov,
                    COALESCE(AVG(daily_cogs), 0) as avg_cogs
                FROM (
                    SELECT 
                        o.order_date,
                        SUM(o.total_price) as daily_revenue,
                        COUNT(DISTINCT o.order_id) as daily_orders,
                        AVG(o.total_price) as daily_aov,
                        COALESCE(SUM(oli.quantity * pv.cost), 0) as daily_cogs
                    FROM shopify.orders o
                    LEFT JOIN shopify.order_line_items oli 
                        ON o.shop_id = oli.shop_id AND o.order_id = oli.order_id
                    LEFT JOIN shopify.product_variants pv 
                        ON oli.shop_id = pv.shop_id 
                        AND oli.product_id = pv.product_id 
                        AND oli.variant_id = pv.variant_id
                    WHERE o.shop_id = %s
                      AND o.order_date >= CURRENT_DATE - 30
                      AND o.financial_status IN ('paid', 'PAID','partially_paid','PARTIALLY_PAID')
                    GROUP BY o.order_date
                ) daily_stats
                """,
                (shop_id,)
            )
            
            baseline = await cur.fetchone()
    
    if not baseline or baseline[0] == 0:
        raise HTTPException(404, "No recent data for preview")
    
    avg_revenue, avg_orders, avg_aov, avg_cogs = baseline
    
    # Calculate effects
    price_change_pct = price_multiplier - 1.0
    demand_effect = calculate_price_elasticity_effect(price_change_pct, elasticity)
    
    # New projected values
    new_aov = float(avg_aov) * price_multiplier
    new_orders = float(avg_orders) * (1 + demand_effect)
    new_revenue = new_aov * new_orders
    
    # COGS per order stays constant (price increase doesn't change cost)
    cogs_per_order = float(avg_cogs) / float(avg_orders) if avg_orders > 0 else 0
    new_cogs = cogs_per_order * new_orders
    
    # Profit calculations
    old_profit = float(avg_revenue) - float(avg_cogs)
    new_profit = new_revenue - new_cogs
    profit_change = new_profit - old_profit
    
    # Break-even elasticity (at what elasticity would revenue stay the same?)
    # Revenue = Price * Quantity
    # For revenue to stay same: new_price * new_quantity = old_price * old_quantity
    # (1 + price_change) * (1 + elasticity * price_change) = 1
    # Solving: elasticity_breakeven = -1 / (1 + price_change) for price_change != -1
    if price_change_pct != 0:
        breakeven_elasticity = -1 / price_multiplier
    else:
        breakeven_elasticity = None
    
    return {
        "price_multiplier": price_multiplier,
        "price_change_percent": round(price_change_pct * 100, 2),
        "elasticity": elasticity,
        "current": {
            "daily_revenue": round(float(avg_revenue), 2),
            "daily_orders": round(float(avg_orders), 2),
            "average_order_value": round(float(avg_aov), 2),
            "daily_profit": round(old_profit, 2)
        },
        "projected": {
            "daily_revenue": round(new_revenue, 2),
            "daily_orders": round(new_orders, 2),
            "average_order_value": round(new_aov, 2),
            "daily_profit": round(new_profit, 2)
        },
        "changes": {
            "revenue_change_percent": round((new_revenue / float(avg_revenue) - 1) * 100, 2) if avg_revenue > 0 else 0,
            "orders_change_percent": round(demand_effect * 100, 2),
            "profit_change_absolute": round(profit_change, 2),
            "profit_change_percent": round((profit_change / old_profit) * 100, 2) if old_profit > 0 else 0
        },
        "analysis": {
            "breakeven_elasticity": round(breakeven_elasticity, 3) if breakeven_elasticity else None,
            "is_profitable_change": profit_change > 0,
            "recommendation": get_price_recommendation(price_change_pct, demand_effect, profit_change, old_profit)
        }
    }


def get_price_recommendation(price_change: float, demand_effect: float, profit_change: float, old_profit: float) -> str:
    """Generate a recommendation based on price change analysis"""
    
    if price_change == 0:
        return "No price change - baseline scenario"
    
    profit_change_pct = (profit_change / old_profit * 100) if old_profit > 0 else 0
    
    if price_change > 0:  # Price increase
        if profit_change > 0:
            if profit_change_pct > 10:
                return f"✅ Strong opportunity: {price_change*100:.0f}% price increase could boost profit by {profit_change_pct:.1f}%"
            else:
                return f"✅ Modest gain: Price increase yields {profit_change_pct:.1f}% profit improvement"
        else:
            return f"⚠️ Caution: Demand drop ({demand_effect*100:.1f}%) may outweigh price benefit"
    else:  # Price decrease
        if profit_change > 0:
            return f"✅ Volume play works: Lower price drives enough volume for {profit_change_pct:.1f}% profit gain"
        else:
            return f"⚠️ Not recommended: Volume increase doesn't offset margin loss"


@router.get("/what-if/presets")
async def get_preset_scenarios(
    shop_domain: str = Depends(get_shop_from_token)
):
    """
    Get preset what-if scenarios for quick testing.
    
    Returns common scenarios like:
    - Optimistic growth
    - Pessimistic downturn  
    - Conservative estimate
    - Holiday season
    - Price optimization scenarios
    """
    
    return {
        "presets": [
            {
                "name": "Optimistic Growth",
                "description": "Strong growth with improved margins",
                "icon": "📈",
                "variables": {
                    "revenue_growth": 0.20,
                    "aov_change": 0.10,
                    "order_volume_change": 0.15,
                    "cogs_change": -0.05,
                    "conversion_rate_change": 0.10,
                    "price_multiplier": 1.0,
                    "price_elasticity": -1.5
                }
            },
            {
                "name": "Pessimistic Downturn",
                "description": "Market challenges and increased costs",
                "icon": "📉",
                "variables": {
                    "revenue_growth": -0.10,
                    "aov_change": -0.05,
                    "order_volume_change": -0.15,
                    "cogs_change": 0.10,
                    "conversion_rate_change": -0.05,
                    "price_multiplier": 1.0,
                    "price_elasticity": -1.5
                }
            },
            {
                "name": "Conservative Realistic",
                "description": "Moderate growth based on trends",
                "icon": "📊",
                "variables": {
                    "revenue_growth": 0.05,
                    "aov_change": 0.02,
                    "order_volume_change": 0.03,
                    "cogs_change": 0.03,
                    "conversion_rate_change": 0.01,
                    "price_multiplier": 1.0,
                    "price_elasticity": -1.5
                }
            },
            {
                "name": "Holiday Season Push",
                "description": "Seasonal spike with discounts",
                "icon": "🎄",
                "variables": {
                    "revenue_growth": 0.40,
                    "aov_change": -0.10,
                    "order_volume_change": 0.60,
                    "cogs_change": 0.05,
                    "conversion_rate_change": 0.20,
                    "price_multiplier": 0.85,  # 15% discount
                    "price_elasticity": -2.0  # Higher elasticity during holidays
                }
            },
            {
                "name": "Cost Optimization",
                "description": "Focus on reducing COGS and improving margins",
                "icon": "💰",
                "variables": {
                    "revenue_growth": 0.0,
                    "aov_change": 0.0,
                    "order_volume_change": 0.0,
                    "cogs_change": -0.15,
                    "conversion_rate_change": 0.0,
                    "price_multiplier": 1.0,
                    "price_elasticity": -1.5
                }
            },
            {
                "name": "Market Expansion",
                "description": "New markets, higher acquisition costs",
                "icon": "🌍",
                "variables": {
                    "revenue_growth": 0.30,
                    "aov_change": -0.05,
                    "order_volume_change": 0.40,
                    "cogs_change": 0.08,
                    "conversion_rate_change": 0.05,
                    "price_multiplier": 1.0,
                    "price_elasticity": -1.5
                }
            },
            {
                "name": "Premium Positioning",
                "description": "10% price increase for premium brand",
                "icon": "💎",
                "variables": {
                    "revenue_growth": 0.0,
                    "aov_change": 0.0,
                    "order_volume_change": 0.0,
                    "cogs_change": 0.0,
                    "conversion_rate_change": 0.0,
                    "price_multiplier": 1.10,  # 10% price increase
                    "price_elasticity": -1.0  # Low elasticity (premium/loyal customers)
                }
            },
            {
                "name": "Volume Strategy",
                "description": "15% price cut to drive volume",
                "icon": "🚀",
                "variables": {
                    "revenue_growth": 0.0,
                    "aov_change": 0.0,
                    "order_volume_change": 0.0,
                    "cogs_change": 0.0,
                    "conversion_rate_change": 0.0,
                    "price_multiplier": 0.85,  # 15% price decrease
                    "price_elasticity": -2.0  # High elasticity (price-sensitive market)
                }
            },
            {
                "name": "Inflation Adjustment",
                "description": "5% price increase to offset rising costs",
                "icon": "📈",
                "variables": {
                    "revenue_growth": 0.0,
                    "aov_change": 0.0,
                    "order_volume_change": 0.0,
                    "cogs_change": 0.05,  # COGS up 5%
                    "conversion_rate_change": 0.0,
                    "price_multiplier": 1.05,  # Price up 5% to match
                    "price_elasticity": -1.2  # Moderate elasticity
                }
            }
        ]
    }


def generate_insights(
    revenue_stats: Dict,
    profit_stats: Dict,
    prob_positive: float,
    sensitivity: Dict[str, float],
    variables: WhatIfVariables,
    price_change_pct: float = 0.0,
    demand_effect: float = 0.0
) -> List[str]:
    """Generate natural language insights from simulation results"""
    
    insights = []
    
    # Price change insight (if applicable)
    if price_change_pct != 0:
        direction = "increase" if price_change_pct > 0 else "decrease"
        insights.append(
            f"💵 {abs(price_change_pct)*100:.0f}% price {direction} → "
            f"{abs(demand_effect)*100:.1f}% {'decrease' if demand_effect < 0 else 'increase'} in orders "
            f"(elasticity: {variables.price_elasticity})"
        )
    
    # Probability insight
    if prob_positive >= 0.90:
        insights.append(f"Very high probability ({prob_positive*100:.0f}%) of positive profit - low risk scenario")
    elif prob_positive >= 0.75:
        insights.append(f"Good probability ({prob_positive*100:.0f}%) of positive profit - moderate risk")
    elif prob_positive >= 0.50:
        insights.append(f"Moderate probability ({prob_positive*100:.0f}%) of positive profit - higher risk scenario")
    else:
        insights.append(f"⚠️ Low probability ({prob_positive*100:.0f}%) of positive profit - high risk scenario")
    
    # Find most sensitive variable
    if sensitivity:
        most_sensitive = max(sensitivity.items(), key=lambda x: x[1])
        var_name = most_sensitive[0].replace('_', ' ').title()
        insights.append(f"Most sensitive to: {var_name} ({most_sensitive[1]:.1f}% impact)")
    
    # Revenue range insight
    revenue_range = revenue_stats['percentile_95'] - revenue_stats['percentile_5']
    revenue_uncertainty = (revenue_range / revenue_stats['median']) * 100 if revenue_stats['median'] > 0 else 0
    
    if revenue_uncertainty < 30:
        insights.append(f"Low uncertainty: 90% of outcomes within ±{revenue_uncertainty:.0f}% of median")
    elif revenue_uncertainty < 50:
        insights.append(f"Moderate uncertainty: Results could vary by ±{revenue_uncertainty:.0f}%")
    else:
        insights.append(f"High uncertainty: Wide range of possible outcomes (±{revenue_uncertainty:.0f}%)")
    
    # COGS optimization opportunity
    if variables.cogs_change > 0:
        potential_savings = profit_stats['median'] * (variables.cogs_change / (1 + variables.cogs_change))
        insights.append(f"💡 Reducing COGS by {abs(variables.cogs_change)*100:.0f}% would save ${abs(potential_savings):,.0f}")
    elif variables.cogs_change < 0:
        realized_savings = profit_stats['median'] * abs(variables.cogs_change)
        insights.append(f"✅ COGS reduction improving profit by ~${realized_savings:,.0f}")
    
    return insights