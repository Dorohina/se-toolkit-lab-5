"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict[str, Any]]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]


async def fetch_logs(since: datetime | None = None) -> list[dict[str, Any]]:
    """Fetch check results from the autochecker API."""
    all_logs: list[dict[str, Any]] = []
    base_url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)

    while True:
        params: dict[str, str | int] = {"limit": 500}
        if since is not None:
            params["since"] = since.isoformat()

        async with httpx.AsyncClient() as client:
            response = await client.get(base_url, params=params, auth=auth)
            response.raise_for_status()
            data = response.json()

        logs = data.get("logs", [])
        all_logs.extend(logs)

        if not data.get("has_more", False) or not logs:
            break

        # Use the last log's submitted_at as the new since
        since = datetime.fromisoformat(logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(
    items: list[dict[str, Any]], session: AsyncSession
) -> int:
    """Load items (labs and tasks) into the database."""
    from app.models.item import ItemRecord
    from sqlmodel import select

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # Process labs first
    for item in items:
        if item.get("type") != "lab":
            continue

        title = str(item["title"])
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == title,
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing is None:
            new_item = ItemRecord(type="lab", title=title)
            session.add(new_item)
            await session.flush()  # Get the ID
            existing = new_item
            new_count += 1

        # Map short lab ID (e.g., "lab-01") to the record
        lab_map[str(item["lab"])] = existing

    # Process tasks
    for item in items:
        if item.get("type") != "task":
            continue

        title = str(item["title"])
        lab_short_id = str(item["lab"])
        parent_lab = lab_map.get(lab_short_id)

        if parent_lab is None:
            continue  # Skip if parent lab not found

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == title,
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing is None:
            new_item = ItemRecord(
                type="task",
                title=title,
                parent_id=parent_lab.id,
            )
            session.add(new_item)
            await session.flush()
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict[str, Any]],
    items_catalog: list[dict[str, Any]],
    session: AsyncSession,
) -> int:
    """Load interaction logs into the database."""
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner
    from sqlmodel import select

    # Build lookup: (lab_short_id, task_short_id) -> title
    item_title_map: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_key = str(item["lab"])
        task_key = item.get("task")
        if task_key is not None:
            task_key = str(task_key)
        title = str(item["title"])
        item_title_map[(lab_key, task_key)] = title

    new_count = 0

    for log in logs:
        # 1. Find or create Learner
        student_id = str(log["student_id"])
        stmt = select(Learner).where(Learner.external_id == student_id)
        result = await session.exec(stmt)
        learner = result.first()

        if learner is None:
            learner = Learner(
                external_id=student_id,
                student_group=str(log.get("group", "")),
            )
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        lab_key = str(log["lab"])
        task_key = log.get("task")
        if task_key is not None:
            task_key = str(task_key)
        item_title = item_title_map.get((lab_key, task_key))

        if item_title is None:
            continue  # Skip if no matching item

        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.exec(stmt)
        item = result.first()

        if item is None:
            continue  # Skip if item not in DB

        # 3. Check if InteractionLog already exists (idempotent upsert)
        external_id = int(log["id"])
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == external_id
        )
        result = await session.exec(stmt)
        existing = result.first()

        if existing is not None:
            continue  # Skip if already exists

        # 4. Create InteractionLog
        submitted_at = datetime.fromisoformat(str(log["submitted_at"]))
        assert learner.id is not None
        assert item.id is not None
        new_log = InteractionLog(
            external_id=external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=submitted_at,
        )
        session.add(new_log)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline."""
    from app.models.interaction import InteractionLog
    from sqlmodel import select

    # Step 1: Fetch and load items
    items = await fetch_items()
    await load_items(items, session)

    # Step 2: Determine last synced timestamp
    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc())  # type: ignore[arg-type]
    result = await session.exec(stmt)
    last_record = result.first()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch and load logs
    logs = await fetch_logs(since=since)
    new_logs_count = await load_logs(logs, items, session)

    # Get total count
    total_stmt = select(InteractionLog)
    total_result = await session.exec(total_stmt)
    total_records = len(total_result.all())

    return {
        "new_records": new_logs_count,
        "total_records": total_records,
    }
