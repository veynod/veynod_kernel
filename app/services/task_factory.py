"""Shared task insertion used by the admin API and the cold-start scanner."""

from __future__ import annotations

from typing import Iterable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def insert_task(
    session: AsyncSession,
    *,
    workspace_id: UUID,
    title: str,
    spec_prompt: str,
    task_type: str,
    read_artifact_ids: Iterable[UUID] = (),
    target_artifact_ids: Iterable[UUID] = (),
    test_artifact_ids: Iterable[UUID] = (),
    write_locks: Iterable[str] = (),
    ancestor_locks: Iterable[str] = (),
    depends_on_task_ids: Iterable[UUID] = (),
    priority: int = 100,
    max_retries: int = 3,
    parent_task_id: UUID | None = None,
) -> UUID:
    dependencies = list(dict.fromkeys(depends_on_task_ids))
    in_degree = len(dependencies)
    task_status = "READY" if in_degree == 0 else "BLOCKED"

    task_id = (
        await session.execute(
            text(
                """
                INSERT INTO dag_tasks (
                    workspace_id, parent_task_id, title, spec_prompt, type, status,
                    priority, in_degree, read_artifact_ids, target_artifact_ids,
                    test_artifact_ids, write_locks, ancestor_locks, max_retries
                ) VALUES (
                    :workspace_id, :parent_task_id, :title, :spec,
                    CAST(:task_type AS task_type), CAST(:status AS task_status),
                    :priority, :in_degree, CAST(:read_ids AS UUID[]),
                    CAST(:target_ids AS UUID[]), CAST(:test_ids AS UUID[]),
                    CAST(:write_locks AS TEXT[]), CAST(:ancestor_locks AS TEXT[]),
                    :max_retries
                )
                RETURNING task_id
                """
            ),
            {
                "workspace_id": workspace_id,
                "parent_task_id": parent_task_id,
                "title": title[:256],
                "spec": spec_prompt,
                "task_type": task_type,
                "status": task_status,
                "priority": priority,
                "in_degree": in_degree,
                "read_ids": [str(value) for value in read_artifact_ids],
                "target_ids": [str(value) for value in target_artifact_ids],
                "test_ids": [str(value) for value in test_artifact_ids],
                "write_locks": list(write_locks),
                "ancestor_locks": list(ancestor_locks),
                "max_retries": max_retries,
            },
        )
    ).scalar_one()

    for upstream in dependencies:
        await session.execute(
            text(
                "INSERT INTO task_dependencies (upstream_task_id, downstream_task_id) "
                "VALUES (:upstream, :downstream) ON CONFLICT DO NOTHING"
            ),
            {"upstream": upstream, "downstream": task_id},
        )
    return task_id


__all__ = ["insert_task"]
