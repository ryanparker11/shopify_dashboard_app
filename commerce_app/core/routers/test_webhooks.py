"""
Test script for Shopify webhook endpoints.
Run with: python test_webhooks.py
"""

import requests
import json
import hmac
import hashlib
import base64
import os
from datetime import datetime

# Configuration
BASE_URL = "http://api.lodestaranalytics.io"  # Change to your FastAPI server URL
WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "your-test-secret")
SHOP_DOMAIN = "test-shop.myshopify.com"


def generate_hmac(body: str, secret: str) -> str:
    """Generate HMAC signature for webhook body."""
    return base64.b64encode(
        hmac.new(
            secret.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        ).digest()
    ).decode('utf-8')


def test_order_webhook():
    """Test orders/create webhook."""
    print("\n🧪 Testing orders/create webhook...")
    
    # Sample Shopify order payload (simplified)
    payload = {
        "id": 5678901234,
        "order_number": 1001,
        "email": "customer@example.com",
        "created_at": "2025-10-24T10:30:00-04:00",
        "updated_at": "2025-10-24T10:30:00-04:00",
        "total_price": "199.99",
        "subtotal_price": "179.99",
        "total_tax": "20.00",
        "currency": "USD",
        "financial_status": "paid",
        "fulfillment_status": None,
        "customer": {
            "id": 1234567890,
            "email": "customer@example.com",
            "first_name": "John",
            "last_name": "Doe"
        },
        "line_items": [
            {
                "id": 9876543210,
                "title": "Test Product",
                "quantity": 2,
                "price": "89.99"
            }
        ]
    }
    
    body = json.dumps(payload)
    hmac_signature = generate_hmac(body, WEBHOOK_SECRET)
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Topic": "orders/create",
        "X-Shopify-Shop-Domain": SHOP_DOMAIN,
        "X-Shopify-Hmac-Sha256": hmac_signature
    }
    
    response = requests.post(
        f"{BASE_URL}/webhooks/ingest",
        headers=headers,
        data=body
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    if response.status_code == 200:
        print("✅ Order webhook test passed!")
    else:
        print("❌ Order webhook test failed!")
    
    return response.status_code == 200


def test_product_webhook():
    """Test products/create webhook."""
    print("\n🧪 Testing products/create webhook...")
    
    payload = {
        "id": 7890123456,
        "title": "Test Product",
        "handle": "test-product",
        "vendor": "Test Vendor",
        "product_type": "Test Type",
        "tags": "test, sample",
        "status": "active",
        "created_at": "2025-10-24T10:30:00-04:00",
        "updated_at": "2025-10-24T10:30:00-04:00",
        "variants": [
            {
                "id": 1111111111,
                "title": "Default",
                "price": "89.99",
                "sku": "TEST-001"
            }
        ]
    }
    
    body = json.dumps(payload)
    hmac_signature = generate_hmac(body, WEBHOOK_SECRET)
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Topic": "products/create",
        "X-Shopify-Shop-Domain": SHOP_DOMAIN,
        "X-Shopify-Hmac-Sha256": hmac_signature
    }
    
    response = requests.post(
        f"{BASE_URL}/webhooks/ingest",
        headers=headers,
        data=body
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    if response.status_code == 200:
        print("✅ Product webhook test passed!")
    else:
        print("❌ Product webhook test failed!")
    
    return response.status_code == 200


def test_customer_webhook():
    """Test customers/create webhook."""
    print("\n🧪 Testing customers/create webhook...")
    
    payload = {
        "id": 3456789012,
        "email": "newcustomer@example.com",
        "first_name": "Jane",
        "last_name": "Smith",
        "phone": "+1234567890",
        "total_spent": "0.00",
        "orders_count": 0,
        "state": "enabled",
        "created_at": "2025-10-24T10:30:00-04:00",
        "updated_at": "2025-10-24T10:30:00-04:00"
    }
    
    body = json.dumps(payload)
    hmac_signature = generate_hmac(body, WEBHOOK_SECRET)
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Topic": "customers/create",
        "X-Shopify-Shop-Domain": SHOP_DOMAIN,
        "X-Shopify-Hmac-Sha256": hmac_signature
    }
    
    response = requests.post(
        f"{BASE_URL}/webhooks/ingest",
        headers=headers,
        data=body
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    if response.status_code == 200:
        print("✅ Customer webhook test passed!")
    else:
        print("❌ Customer webhook test failed!")
    
    return response.status_code == 200


def test_invalid_hmac():
    """Test webhook with invalid HMAC (should fail)."""
    print("\n🧪 Testing invalid HMAC (should reject)...")
    
    payload = {"id": 123, "test": "data"}
    body = json.dumps(payload)
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Topic": "orders/create",
        "X-Shopify-Shop-Domain": SHOP_DOMAIN,
        "X-Shopify-Hmac-Sha256": "invalid-signature"
    }
    
    response = requests.post(
        f"{BASE_URL}/webhooks/ingest",
        headers=headers,
        data=body
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    
    if response.status_code == 401:
        print("✅ HMAC validation test passed (correctly rejected)!")
    else:
        print("❌ HMAC validation test failed (should have rejected)!")
    
    return response.status_code == 401


def test_webhook_status():
    """Test the webhook status endpoint."""
    print("\n🧪 Testing webhook status endpoint...")
    
    response = requests.get(
        f"{BASE_URL}/webhooks/status",
        params={"shop_domain": SHOP_DOMAIN, "limit": 10}
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    
    if response.status_code == 200:
        print("✅ Webhook status test passed!")
    else:
        print("❌ Webhook status test failed!")
    
    return response.status_code == 200


def test_analytics_after_webhook():
    """Test that analytics endpoint reflects webhook data."""
    print("\n🧪 Testing analytics after webhook ingestion...")
    
    response = requests.get(
        f"{BASE_URL}/orders/summary",
        params={"shop_domain": SHOP_DOMAIN}
    )
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Total Orders: {data['total_orders']}")
        print(f"Total Revenue: ${data['total_revenue']}")
        print(f"Avg Order Value: ${data['avg_order_value']}")
        print("✅ Analytics endpoint working!")
    else:
        print(f"Response: {response.json()}")
        print("❌ Analytics endpoint test failed!")
    
    return response.status_code == 200


def run_all_tests():
    """Run all webhook tests."""
    print("=" * 60)
    print("🚀 Starting Webhook Tests")
    print("=" * 60)
    print(f"Base URL: {BASE_URL}")
    print(f"Shop Domain: {SHOP_DOMAIN}")
    print(f"Webhook Secret: {WEBHOOK_SECRET[:10]}...")
    
    results = []
    
    # Run tests
    results.append(("Order Webhook", test_order_webhook()))
    results.append(("Product Webhook", test_product_webhook()))
    results.append(("Customer Webhook", test_customer_webhook()))
    results.append(("Invalid HMAC", test_invalid_hmac()))
    results.append(("Webhook Status", test_webhook_status()))
    results.append(("Analytics Integration", test_analytics_after_webhook()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()