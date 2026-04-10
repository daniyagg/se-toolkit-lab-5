"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case, text
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


def _lab_title(lab: str) -> str:
    """Transform a lab identifier like 'lab-04' into a title fragment like 'Lab 04'."""
    parts = lab.split("-", 1)
    if len(parts) == 2:
        return f"{parts[0].capitalize()} {parts[1]}"
    return lab.capitalize()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    title_fragment = _lab_title(lab)

    # Find the lab
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        col(ItemRecord.title).contains(title_fragment),
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()
    if not lab_item:
        return []

    # Find tasks belonging to this lab
    tasks_stmt = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id,
    )
    task_ids = [row for row in (await session.exec(tasks_stmt)).all()]
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions with scores for these tasks
    bucket_expr = case(
        (col(InteractionLog.score) <= 25, "0-25"),
        (col(InteractionLog.score) <= 50, "26-50"),
        (col(InteractionLog.score) <= 75, "51-75"),
        else_="76-100",
    )

    stmt = (
        select(bucket_expr.label("bucket"), func.count().label("count"))
        .where(
            col(InteractionLog.item_id).in_(task_ids),
            col(InteractionLog.score).isnot(None),
        )
        .group_by("bucket")
    )
    result = await session.exec(stmt)
    counts = {row[0]: row[1] for row in result.all()}

    return [
        {"bucket": "0-25", "count": counts.get("0-25", 0)},
        {"bucket": "26-50", "count": counts.get("26-50", 0)},
        {"bucket": "51-75", "count": counts.get("51-75", 0)},
        {"bucket": "76-100", "count": counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    title_fragment = _lab_title(lab)

    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        col(ItemRecord.title).contains(title_fragment),
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()
    if not lab_item:
        return []

    tasks_stmt = select(ItemRecord).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id,
    ).order_by(col(ItemRecord.title))
    tasks = (await session.exec(tasks_stmt)).all()

    result = []
    for task in tasks:
        stmt = (
            select(
                func.round(func.avg(col(InteractionLog.score)), 1).label("avg_score"),
                func.count().label("attempts"),
            )
            .where(
                InteractionLog.item_id == task.id,
                col(InteractionLog.score).isnot(None),
            )
        )
        row = (await session.exec(stmt)).first()
        if row and row[1] > 0:
            result.append({
                "task": task.title,
                "avg_score": float(row[0]),
                "attempts": row[1],
            })
        else:
            result.append({
                "task": task.title,
                "avg_score": 0.0,
                "attempts": 0,
            })

    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    title_fragment = _lab_title(lab)

    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        col(ItemRecord.title).contains(title_fragment),
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()
    if not lab_item:
        return []

    tasks_stmt = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id,
    )
    task_ids = [row for row in (await session.exec(tasks_stmt)).all()]
    if not task_ids:
        return []

    stmt = (
        select(
            func.date(col(InteractionLog.created_at)).label("date"),
            func.count().label("submissions"),
        )
        .where(col(InteractionLog.item_id).in_(task_ids))
        .group_by("date")
        .order_by(text("date ASC"))
    )
    result = await session.exec(stmt)
    return [
        {"date": str(row[0]), "submissions": row[1]}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    title_fragment = _lab_title(lab)

    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab",
        col(ItemRecord.title).contains(title_fragment),
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()
    if not lab_item:
        return []

    tasks_stmt = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id,
    )
    task_ids = [row for row in (await session.exec(tasks_stmt)).all()]
    if not task_ids:
        return []

    stmt = (
        select(
            col(Learner.student_group).label("group"),
            func.round(func.avg(col(InteractionLog.score)), 1).label("avg_score"),
            func.count(func.distinct(col(Learner.id))).label("students"),
        )
        .join(Learner, col(Learner.id) == col(InteractionLog.learner_id))
        .where(
            col(InteractionLog.item_id).in_(task_ids),
            col(InteractionLog.score).isnot(None),
        )
        .group_by(col(Learner.student_group))
        .order_by(col(Learner.student_group).asc())
    )
    result = await session.exec(stmt)
    return [
        {
            "group": row[0],
            "avg_score": float(row[1]),
            "students": row[2],
        }
        for row in result.all()
    ]
