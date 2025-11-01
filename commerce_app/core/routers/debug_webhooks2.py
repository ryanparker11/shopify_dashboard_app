import os, hmac, hashlib, base64, requests, hashlib as _hash

URL = "https://api.lodestaranalytics.io/webhooks/ingest"
SECRET = "731897f72a71968f131433a1cb1413f0"  # must match server exactly

# 1) Log secret fingerprint
print("CLIENT secret prefix:", SECRET[:6], " sha256:", _hash.sha256(SECRET.encode()).hexdigest()[:12])

# 2) Use a literal payload string (NO dumps; NO pretty print)
payload_str = '{"id":123,"order_number":1001,"email":"buyer@example.com","name":"#1001","total_price":"19.99","currency":"USD","created_at":"2025-10-31T16:00:00Z","updated_at":"2025-10-31T16:00:00Z","line_items":[]}'

# 3) Sign the exact bytes we will send
body = payload_str.encode("utf-8")
sig  = base64.b64encode(hmac.new(SECRET.encode("utf-8"), body, hashlib.sha256).digest()).decode("utf-8")
print("CLIENT computed HMAC:", sig)

headers = {
  "Content-Type": "application/json",
  "X-Shopify-Topic": "orders/create",
  "X-Shopify-Shop-Domain": "dashboard-mvp.myshopify.com",
  "X-Shopify-Hmac-Sha256": sig
}

r = requests.post(URL, data=body, headers=headers, timeout=10)
print(r.status_code, r.text)
