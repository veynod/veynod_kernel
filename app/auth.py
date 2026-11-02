"""Machine (Runner) token authentication.

Tokens are opaque ``vnk_...`` strings; only their SHA-256 hash is stored. The
worker hub id is deterministically derived from the hash, which keeps the same
runner identity across reconnects and matches the legacy v3 behaviour.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import AgentToken

TOKEN_PREFIX = "vnk_"
_TOKEN_CACHE_TTL = 5.0
_token_cache: dict[str, tuple[str, float]] = {}


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def derive_worker_hub_id(raw_token: str) -> str:
    return f"hub_{hash_token(raw_token)[:16]}"


def new_token() -> str:
    return TOKEN_PREFIX + secrets.token_urlsafe(32)


async def register_token(
    session: AsyncSession, raw_token: str, label: str = "runner"
) -> AgentToken:
    """Idempotently register an existing raw token."""
    token_hash = hash_token(raw_token)
    existing = (
        await session.execute(select(AgentToken).where(AgentToken.token_hash == token_hash))
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    record = AgentToken(
        worker_hub_id=derive_worker_hub_id(raw_token),
        label=label,
        token_hash=token_hash,
        active=True,
    )
    session.add(record)
    await session.flush()
    return record


async def issue_token(session: AsyncSession, label: str = "runner") -> tuple[str, AgentToken]:
    raw = new_token()
    record = await register_token(session, raw, label)
    await session.commit()
    return raw, record


async def authenticate_worker_token(
    session: AsyncSession, raw_token: str
) -> str | None:
    """Return the worker hub id for a valid token, or None.

    In ``dev`` mode an unknown but well-formed ``vnk_`` token is still accepted
    and hashed into a hub id, mirroring the original v3 port. Production must
    run ``VN_AUTH_MODE=prod`` and issue tokens through the admin API.
    """
    if not raw_token.startswith(TOKEN_PREFIX):
        return None

    token_hash = hash_token(raw_token)
    now = time.monotonic()
    cached = _token_cache.get(token_hash)
    if cached is not None and cached[1] > now:
        return cached[0]

    started = time.perf_counter()
    record = (
        await session.execute(select(AgentToken).where(AgentToken.token_hash == token_hash))
    ).scalar_one_or_none()
    try:
        from app.services.runtime_metrics import get_runtime_metrics

        get_runtime_metrics().observe(
            "auth.ms", (time.perf_counter() - started) * 1000
        )
    except Exception:  # noqa: BLE001 - metrics must not break auth
        pass

    if record is not None:
        if not record.active:
            return None
        # Presence is intentionally not written on every request: the auth
        # dependency runs for every pull/submit and a per-request commit is a
        # DB write hotspot. Heartbeats/leases are the liveness signal.
        if len(_token_cache) > 20_000:
            _token_cache.clear()
        _token_cache[token_hash] = (record.worker_hub_id, now + _TOKEN_CACHE_TTL)
        return record.worker_hub_id

    if settings.auth_mode == "dev":
        return derive_worker_hub_id(raw_token)
    return None


def constant_time_equals(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))
