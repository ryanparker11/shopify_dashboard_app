# commerce_app/auth/session_tokens.py
import os, base64, json, hmac, hashlib, time
from typing import Dict, Any
from fastapi import Header, HTTPException
from fastapi.responses import JSONResponse

SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")           # optional audience check
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]         # required

def _b64url_decode(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())

def verify_shopify_session_token(authorization: str = Header(None)) -> Dict[str, Any]:
    """
    Verifies Shopify session token (HS256 using SHOPIFY_API_SECRET).
    Returns decoded payload on success.
    On failure, returns 401 JSONResponse with X-Shopify-Retry-Invalid-Session-Request header
    so App Bridge can automatically retry with a fresh token.
    """
    
    # Check if Authorization header is present
    if not authorization:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Missing authorization header"}
        )
    
    if not authorization.startswith("Bearer "):
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Missing Bearer token"}
        )

    token = authorization.split(" ", 1)[1]

    # Parse JWT (no external libs required)
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Malformed token"}
        )

    # Verify signature
    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected = hmac.new(SHOPIFY_API_SECRET.encode(), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, _b64url_decode(sig_b64)):
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Invalid signature"}
        )

    # Validate claims
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Invalid payload"}
        )

    # Check token expiration
    now = int(time.time())
    if payload.get("nbf", 0) > now:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Token not yet valid"}
        )
    
    if payload.get("exp", 0) <= now:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Token expired"}
        )

    # Validate issuer
    iss = str(payload.get("iss", ""))
    if not iss.endswith(".myshopify.com/admin"):
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Invalid issuer"}
        )

    # Validate audience if API key is set
    aud = payload.get("aud")
    if aud and SHOPIFY_API_KEY and aud != SHOPIFY_API_KEY:
        return JSONResponse(
            status_code=401,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"},
            content={"detail": "Invalid audience"}
        )

    return payload  # includes 'sub' (shop id), 'dest', etc.
