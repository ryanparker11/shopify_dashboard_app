@router.get("/customers/summary")
def customers_summary():
    # Replace with DB query later
    return {
        "total_customers": 11872,
        "new_customers_last_30d": 842,
        "repeat_purchase_rate": 0.37
    }
