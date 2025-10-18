from datetime import date, timedelta

@router.get("/sales/forecast")
def sales_forecast(days:int=14):
    # naive baseline: last 14 days avg projected forward
    hist = [3000, 3200, 3100, 3400, 3600, 3550, 3700, 3800, 3950, 4100, 3900, 4000, 4200, 4300]
    avg = sum(hist) / len(hist)
    out = []
    for i in range(days):
        d = date.today() + timedelta(days=i+1)
        out.append({"date": d.isoformat(), "forecast_revenue": round(avg,2)})
    return out
