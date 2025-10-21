# commerce_app/core/db.py
from contextlib import asynccontextmanager
from psycopg_pool import AsyncConnectionPool
from .config import DATABASE_DSN

_pool: AsyncConnectionPool | None = None

async def init_pool() -> AsyncConnectionPool:
    """Initialize a single global pool (idempotent)."""
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo=DATABASE_DSN,
            min_size=1,
            max_size=5,
            open=True,   # open immediately; fail-fast if DSN wrong
            timeout=10,
        )
    return _pool

async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None

@asynccontextmanager
async def get_conn():
    """FastAPI dependency helper for a single connection from the pool."""
    global _pool
    if _pool is None:
        await init_pool()
    assert _pool is not None
    async with _pool.connection() as conn:
        yield conn
