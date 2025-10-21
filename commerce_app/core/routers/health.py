from fastapi import APIRouter
from commerce_app.core.db import get_conn

router = APIRouter()

@router.get("/healthz")
async def healthz():
    # simple DB round-trip so ECS health check actually validates connectivity
    async with get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1;")
            _ = await cur.fetchone()
    return {"ok": True}
