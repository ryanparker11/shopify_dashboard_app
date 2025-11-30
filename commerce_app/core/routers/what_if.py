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
                  AND o.financial_status IN ('paid', 'partially_paid')
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
                  AND o.financial_status IN ('paid', 'partially_paid')
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
    
    # Apply what-if adjustments to baseline
    adjusted_daily_revenue = base_daily_revenue * (1 + request.variables.revenue_growth)
    adjusted_daily_orders = base_daily_orders * (1 + request.variables.order_volume_change)
    adjusted_aov = base_aov * (1 + request.variables.aov_change)
    adjusted_cogs_rate = (base_daily_cogs / base_daily_revenue) * (1 + request.variables.cogs_change) if base_daily_revenue > 0 else 0
    
    # Run simulations
    for i in range(n_sims):
        # Sample from normal distributions based on historical volatility
        # Each day in forecast period gets random variation
        daily_sim_revenues = np.random.normal(
            adjusted_daily_revenue, 
            revenue_std * 0.8,  # Reduce volatility slightly for projections
            forecast_days
        )
        
        daily_sim_orders = np.random.normal(
            adjusted_daily_orders,
            order_std * 0.8,
            forecast_days
        )
        
        # Ensure no negative values
        daily_sim_revenues = np.maximum(daily_sim_revenues, 0)
        daily_sim_orders = np.maximum(daily_sim_orders, 0)
        
        # Calculate totals for this simulation
        total_revenue = np.sum(daily_sim_revenues)
        total_orders = np.sum(daily_sim_orders)
        
        # Calculate COGS with adjusted rate
        total_cogs = total_revenue * adjusted_cogs_rate
        
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
    
    # Calculate sensitivity analysis
    sensitivity = calculate_sensitivity(
        simulated_revenues,
        {
            "revenue_growth": request.variables.revenue_growth,
            "aov_change": request.variables.aov_change,
            "order_volume_change": request.variables.order_volume_change,
            "cogs_change": request.variables.cogs_change
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
            request.variables
        )
    }


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
                    "conversion_rate_change": 0.10
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
                    "conversion_rate_change": -0.05
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
                    "conversion_rate_change": 0.01
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
                    "conversion_rate_change": 0.20
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
                    "conversion_rate_change": 0.0
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
                    "conversion_rate_change": 0.05
                }
            }
        ]
    }


def generate_insights(
    revenue_stats: Dict,
    profit_stats: Dict,
    prob_positive: float,
    sensitivity: Dict[str, float],
    variables: WhatIfVariables
) -> List[str]:
    """Generate natural language insights from simulation results"""
    
    insights = []
    
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