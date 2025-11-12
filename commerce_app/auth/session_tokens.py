# commerce_app/auth/session_tokens.py
import os, base64, json, hmac, hashlib, time
from typing import Dict, Any
from fastapi import Header, HTTPException

SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")           # optional audience check
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]         # required

def _b64url_decode(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())

def verify_shopify_session_token(authorization: str = Header(...)) -> Dict[str, Any]:
    """
    Verifies Shopify session token (HS256 using SHOPIFY_API_SECRET).
    Returns decoded payload on success, raises 401 on failure.
    """
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.split(" ", 1)[1]

    # Parse JWT (no external libs required)
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        raise HTTPException(status_code=401, detail="Malformed token")

    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected = hmac.new(SHOPIFY_API_SECRET.encode(), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, _b64url_decode(sig_b64)):
        raise HTTPException(status_code=401, detail="Invalid signature")

    # Validate claims
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid payload")

    now = int(time.time())
    if payload.get("nbf", 0) > now or payload.get("exp", 0) <= now:
        raise HTTPException(status_code=401, detail="Token not yet valid or expired")

    iss = str(payload.get("iss", ""))
    if not iss.endswith(".myshopify.com/admin"):
        raise HTTPException(status_code=401, detail="Invalid issuer")

    aud = payload.get("aud")
    if aud and SHOPIFY_API_KEY and aud != SHOPIFY_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid audience")

    return payload  # includes 'sub' (shop id), 'dest', etc.
