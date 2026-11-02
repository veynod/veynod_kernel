"""Application lifecycle: schema bootstrap, reconciliation and lease reaper."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from sqlalchemy import func, select

from app.auth import register_token
from app.config import settings
from app.database import dispose_database, init_db, session_scope
from app.engine.scheduler import DisjointWriteSetScheduler
from app.engine.scheduler_core import scheduler_core_manager
from app.models import KernelEvent, Workspace
from app.services.event_bus import EventBus
from app.services.event_hub import get_event_hub
from app.services.edge_rebuild import edge_rebuild_service
from app.services.git_ops import GitRepository, close_git_batch_pools
from app.services.outbox import outbox_dispatcher

logger = logging.getLogger("veynod.runtime")


async def _bootstrap_token() -> None:
    token = settings.bootstrap_runner_token
    if not token:
        return
    async with session_scope() as session:
        await register_token(session, token, label="bootstrap-runner")
        await session.commit()
    logger.info("Bootstrap runner token registered.")


async def _reconcile_workspaces() -> None:
    """Git is the source of truth if the process died between update-ref and commit."""
    async with session_scope() as session:
        workspaces = (await session.execute(select(Workspace))).scalars().all()

    for workspace in workspaces:
        repo = GitRepository(workspace.repo_path, workspace.main_branch)
        try:
            head = await repo.head_sha()
        except Exception:  # noqa: BLE001 - a bad repo must not block startup
            logger.warning(
                "Workspace %s repo %s is unreachable; keeping DB head %s.",
                workspace.workspace_id,
                workspace.repo_path,
                workspace.head_commit_sha,
            )
            continue
        if head != workspace.head_commit_sha:
            logger.warning(
                "Reconciling workspace %s head %s -> %s from git.",
                workspace.workspace_id,
                workspace.head_commit_sha,
                head,
            )
            async with session_scope() as session:
                refreshed = await session.get(Workspace, workspace.workspace_id)
                if refreshed is not None:
                    refreshed.head_commit_sha = head
                    await session.commit()


async def _reap_once() -> None:
    async with session_scope() as session:
        workspace_ids = list(
            (
                await session.execute(select(Workspace.workspace_id))
            ).scalars().all()
        )

    for workspace_id in workspace_ids:
        try:
            async with session_scope() as session:
                bus = EventBus(session)
                scheduler = DisjointWriteSetScheduler(session, bus)
                expired = await scheduler.reap_expired_leases(workspace_id)
                stuck = await scheduler.reap_stuck_verifications(
                    workspace_id, settings.reap_verifying_after_seconds
                )
                reaped = [*expired, *stuck]
                for row in reaped:
                    event_type = (
                        "TASK_FAILED" if row["status"] == "FAILED" else "TASK_REAPED"
                    )
                    await bus.publish(
                        workspace_id,
                        event_type,
                        {"task_id": str(row["task_id"]), "new_status": row["status"]},
                    )
                await session.commit()
                if reaped:
                    await scheduler_core_manager.sync_tasks(
                        session, workspace_id, [row["task_id"] for row in reaped]
                    )
        except Exception:  # noqa: BLE001 - one workspace must not kill the loop
            logger.exception("Lease reaper failed for workspace %s", workspace_id)


async def _scheduler_sync_loop() -> None:
    """Feed external gate-worker events back into the API's Rust core.

    The gate runs in a separate process.  Its commits update PostgreSQL and
    kernel_events, but the API's in-memory SchedulerCore would otherwise keep
    the old LEASED task forever.  This loop is a small, bounded bridge; the DB
    remains the authority and SchedulerCore's versioned upsert ignores stale
    snapshots.
    """
    async with session_scope() as session:
        cursor = int(
            await session.scalar(
                select(func.coalesce(func.max(KernelEvent.event_seq), 0))
            )
            or 0
        )
    while True:
        try:
            await asyncio.sleep(0.1)
            async with session_scope() as session:
                rows = (
                    await session.execute(
                        select(KernelEvent)
                        .where(
                            KernelEvent.event_seq > cursor,
                            KernelEvent.event_type.in_(
                                (
                                    "GATE_PASSED",
                                    "TASK_READY",
                                    "TASK_FAILED",
                                    "TASK_REAPED",
                                    "TASK_RELEASED",
                                    "SUBMISSION_COMMITTED",
                                    "SUBMISSION_FAILED",
                                )
                            ),
                        )
                        .order_by(KernelEvent.event_seq.asc())
                        .limit(5000)
                    )
                ).scalars().all()
                by_workspace: dict = {}
                for event in rows:
                    payload = event.payload or {}
                    ids = []
                    if payload.get("task_id"):
                        ids.append(payload["task_id"])
                    ids.extend(payload.get("ready_task_ids") or [])
                    if ids:
                        by_workspace.setdefault(event.workspace_id, set()).update(ids)
                    cursor = max(cursor, event.event_seq)
                for workspace_id, ids in by_workspace.items():
                    await scheduler_core_manager.sync_tasks(
                        session, workspace_id, list(ids)
                    )
                await session.commit()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Scheduler core event sync failed")


async def _reaper_loop() -> None:
    while True:
        await asyncio.sleep(settings.reaper_interval_seconds)
        try:
            await _reap_once()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Lease reaper iteration failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()
    await _bootstrap_token()
    await _reconcile_workspaces()
    scheduler_core_manager.reset()
    await get_event_hub().start()
    if edge_rebuild_service.enabled:
        await edge_rebuild_service.start()
    await outbox_dispatcher.start()
    reaper = asyncio.create_task(_reaper_loop(), name="veynod-lease-reaper")
    core_sync = asyncio.create_task(
        _scheduler_sync_loop(), name="veynod-scheduler-event-sync"
    )
    warmup = asyncio.create_task(
        scheduler_core_manager.warmup(), name="veynod-scheduler-warmup"
    )
    try:
        yield
    finally:
        reaper.cancel()
        core_sync.cancel()
        warmup.cancel()
        with suppress(asyncio.CancelledError):
            await reaper
        with suppress(asyncio.CancelledError):
            await core_sync
        with suppress(asyncio.CancelledError):
            await warmup
        await edge_rebuild_service.stop()
        await outbox_dispatcher.stop()
        await get_event_hub().stop()
        await close_git_batch_pools()
        await dispose_database()


__all__ = ["lifespan"]
