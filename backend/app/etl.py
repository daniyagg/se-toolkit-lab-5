"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch items: {resp.status_code} {resp.text}"
            )
        return resp.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    all_logs: list[dict] = []
    limit = 500

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": limit}
            if since is not None:
                params["since"] = since.isoformat()

            resp = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=(settings.autochecker_email, settings.autochecker_password),
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch logs: {resp.status_code} {resp.text}"
                )

            data = resp.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the last log's submitted_at as the new since value
            if logs:
                last_submitted = logs[-1]["submitted_at"]
                since = datetime.fromisoformat(last_submitted.replace("Z", "+00:00"))
            else:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0

    # Process labs first (type="lab")
    lab_id_map: dict[str, ItemRecord] = {}  # maps short lab ID -> ItemRecord

    for item in items:
        if item["type"] != "lab":
            continue

        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab", ItemRecord.title == item["title"]
            )
        )
        lab_record = existing.first()

        if lab_record is None:
            lab_record = ItemRecord(
                type="lab",
                title=item["title"],
            )
            session.add(lab_record)
            new_count += 1

        # Map the short lab ID (e.g. "lab-01") to the record
        lab_id_map[item["lab"]] = lab_record

    # Flush to get IDs for newly created labs
    await session.flush()

    # Process tasks (type="task")
    for item in items:
        if item["type"] != "task":
            continue

        # Find the parent lab
        parent_lab = lab_id_map.get(item["lab"])
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists with this title and parent_id
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == item["title"],
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_record = existing.first()

        if task_record is None:
            task_record = ItemRecord(
                type="task",
                title=item["title"],
                parent_id=parent_lab.id,
            )
            session.add(task_record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    new_count = 0

    # Build lookup: (lab_short_id, task_short_id) -> title
    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        elif item["type"] == "task":
            title_lookup[(item["lab"], item["task"])] = item["title"]

    for log in logs:
        # 1. Find or create Learner by external_id
        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner = learner_result.first()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()  # Get the learner ID

        # 2. Find the matching item in the database
        lab_short_id = log["lab"]
        task_short_id = log.get("task")
        item_title = title_lookup.get((lab_short_id, task_short_id))

        if item_title is None:
            # No matching item found, skip this log
            continue

        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()

        if item is None:
            # Item not in DB, skip this log
            continue

        # 3. Check if InteractionLog with this external_id already exists
        # Convert log_id to int to match the DB field type
        log_id = int(log["id"])
        existing = await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == log_id
            )
        )
        if existing.first() is not None:
            # Already exists, skip (idempotent upsert)
            continue

        # 4. Create InteractionLog
        submitted_at_str = log["submitted_at"]
        # Handle ISO format with 'Z' suffix
        if submitted_at_str.endswith("Z"):
            submitted_at_str = submitted_at_str[:-1] + "+00:00"
        created_at = datetime.fromisoformat(submitted_at_str)

        interaction = InteractionLog(
            external_id=log_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=created_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    # Step 1: Fetch items and load them into the database
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    last_interaction_result = await session.exec(
        select(InteractionLog).order_by(col(InteractionLog.created_at).desc()).limit(1)
    )
    last_interaction = last_interaction_result.first()
    since = last_interaction.created_at if last_interaction else None

    # Step 3: Fetch logs since that timestamp and load them
    # Pass the raw items list to load_logs so it can map short IDs to titles
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items_catalog, session)

    # Get total interactions count
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
