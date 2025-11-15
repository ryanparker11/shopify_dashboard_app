# commerce_app/auth/session_tokens.py
import os, base64, json, hmac, hashlib, time
from typing import Dict, Any
from fastapi import Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from starlette.responses import Response as StarletteResponse

SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY")
SHOPIFY_API_SECRET = os.environ["SHOPIFY_API_SECRET"]

def _b64url_decode(s: str) -> bytes:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s.encode())

class SessionTokenException(HTTPException):
    """Custom exception that includes the retry header"""
    def __init__(self, detail: str):
        super().__init__(
            status_code=401,
            detail=detail,
            headers={"X-Shopify-Retry-Invalid-Session-Request": "1"}
        )

def verify_shopify_session_token(authorization: str = Header(None)) -> Dict[str, Any]:
    """
    Verifies Shopify session token (HS256 using SHOPIFY_API_SECRET).
    Returns decoded payload on success.
    Raises SessionTokenException with retry header on failure.
    """
    
    # Check if Authorization header is present
    if not authorization:
        raise SessionTokenException("Missing authorization header")
    
    if not authorization.startswith("Bearer "):
        raise SessionTokenException("Missing Bearer token")

    token = authorization.split(" ", 1)[1]

    # Parse JWT
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        raise SessionTokenException("Malformed token")

    # Verify signature
    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected = hmac.new(SHOPIFY_API_SECRET.encode(), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, _b64url_decode(sig_b64)):
        raise SessionTokenException("Invalid signature")

    # Validate claims
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise SessionTokenException("Invalid payload")

    # Check token expiration
    now = int(time.time())
    if payload.get("nbf", 0) > now:
        raise SessionTokenException("Token not yet valid")
    
    if payload.get("exp", 0) <= now:
        raise SessionTokenException("Token expired")

    # Validate issuer
    iss = str(payload.get("iss", ""))
    if not iss.endswith(".myshopify.com/admin"):
        raise SessionTokenException("Invalid issuer")

    # Validate audience if API key is set
    aud = payload.get("aud")
    if aud and SHOPIFY_API_KEY and aud != SHOPIFY_API_KEY:
        raise SessionTokenException("Invalid audience")

    return payload
