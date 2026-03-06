"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Score distribution histogram for a given lab."""
    # Find the lab item by matching title (e.g., "lab-04" → "Lab 04")
    lab_title = lab.replace("-", " ").title()
    stmt = select(ItemRecord).where(
        col(ItemRecord.type) == "lab",
        col(ItemRecord.title).ilike(f"%{lab_title}%"),
    )
    result = await session.exec(stmt)
    lab_item = result.first()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Find all tasks that belong to this lab
    stmt = select(ItemRecord.id).where(
        col(ItemRecord.parent_id) == lab_item.id,
        col(ItemRecord.type) == "task",
    )
    result = await session.exec(stmt)
    task_ids = [r for r in result.all()]

    # Query interactions for these items that have a score
    stmt = select(
        case(
            (col(InteractionLog.score) <= 25, "0-25"),
            (col(InteractionLog.score) <= 50, "26-50"),
            (col(InteractionLog.score) <= 75, "51-75"),
            (col(InteractionLog.score) <= 100, "76-100"),
            else_="0-25",
        ).label("bucket"),
        func.count().label("count"),
    ).where(
        col(InteractionLog.item_id).in_(task_ids + [lab_item.id]),
        col(InteractionLog.score).is_not(None),
    ).group_by("bucket")

    result = await session.exec(stmt)
    rows = result.all()

    # Build result with all buckets
    buckets: dict[str, int] = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for bucket, count in rows:
        buckets[bucket] = count

    return [
        {"bucket": "0-25", "count": buckets["0-25"]},
        {"bucket": "26-50", "count": buckets["26-50"]},
        {"bucket": "51-75", "count": buckets["51-75"]},
        {"bucket": "76-100", "count": buckets["76-100"]},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Per-task pass rates for a given lab."""
    # Find the lab item by matching title
    lab_title = lab.replace("-", " ").title()
    stmt = select(ItemRecord).where(
        col(ItemRecord.type) == "lab",
        col(ItemRecord.title).ilike(f"%{lab_title}%"),
    )
    result = await session.exec(stmt)
    lab_item = result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    stmt = select(ItemRecord).where(
        col(ItemRecord.parent_id) == lab_item.id,
        col(ItemRecord.type) == "task",
    ).order_by(col(ItemRecord.title))
    result = await session.exec(stmt)
    tasks = result.all()

    response: list[dict[str, Any]] = []
    for task in tasks:
        # For each task, compute avg_score and attempts
        stmt = select(
            func.avg(col(InteractionLog.score)).label("avg_score"),
            func.count().label("attempts"),
        ).where(
            col(InteractionLog.item_id) == task.id,
            col(InteractionLog.score).is_not(None),
        )
        result = await session.exec(stmt)
        row = result.first()

        avg_score = round(float(row[0]), 1) if row[0] is not None else 0.0  # type: ignore[index]
        attempts = row[1] or 0  # type: ignore[index]

        response.append({
            "task": task.title,
            "avg_score": avg_score,
            "attempts": attempts,
        })

    return response


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Submissions per day for a given lab."""
    # Find the lab item by matching title
    lab_title = lab.replace("-", " ").title()
    stmt = select(ItemRecord).where(
        col(ItemRecord.type) == "lab",
        col(ItemRecord.title).ilike(f"%{lab_title}%"),
    )
    result = await session.exec(stmt)
    lab_item = result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    stmt = select(ItemRecord.id).where(
        col(ItemRecord.parent_id) == lab_item.id,
        col(ItemRecord.type) == "task",
    )
    result = await session.exec(stmt)
    task_ids = [r for r in result.all()]

    # Group interactions by date
    stmt = select(
        func.date(col(InteractionLog.created_at)).label("date"),
        func.count().label("submissions"),
    ).where(
        col(InteractionLog.item_id).in_(task_ids + [lab_item.id]),
    ).group_by(
        func.date(col(InteractionLog.created_at))
    ).order_by(
        func.date(col(InteractionLog.created_at))
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {"date": str(row[0]), "submissions": row[1]}  # type: ignore[index]
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    """Per-group performance for a given lab."""
    # Find the lab item by matching title
    lab_title = lab.replace("-", " ").title()
    stmt = select(ItemRecord).where(
        col(ItemRecord.type) == "lab",
        col(ItemRecord.title).ilike(f"%{lab_title}%"),
    )
    result = await session.exec(stmt)
    lab_item = result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    stmt = select(ItemRecord.id).where(
        col(ItemRecord.parent_id) == lab_item.id,
        col(ItemRecord.type) == "task",
    )
    result = await session.exec(stmt)
    task_ids = [r for r in result.all()]

    # Join interactions with learners to get student_group
    stmt = select(  # type: ignore[assignment]
        col(Learner.student_group).label("group"),  # type: ignore[arg-type]
        func.avg(col(InteractionLog.score)).label("avg_score"),
        func.count(func.distinct(col(Learner.id))).label("students"),
    ).join(
        InteractionLog, col(InteractionLog.learner_id) == col(Learner.id),
    ).where(
        col(InteractionLog.item_id).in_(task_ids + [lab_item.id]),
        col(InteractionLog.score).is_not(None),
    ).group_by(
        col(Learner.student_group)
    ).order_by(
        col(Learner.student_group)
    )

    result = await session.exec(stmt)  # type: ignore[arg-type]
    rows = result.all()  # type: ignore[var-annotated]

    return [  # pyright: ignore[reportUnknownVariableType]
        {
            "group": row[0],  # type: ignore[index]
            "avg_score": round(float(row[1]), 1) if row[1] is not None else 0.0,  # type: ignore[index]
            "students": row[2],  # type: ignore[index]
        }
        for row in rows
    ]
