import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.database import get_db
from app.models.signal_snapshot import SignalSnapshot
from app.models.technology import Technology
from app.schemas.signal_snapshot import TechnologySignalResponse
from app.schemas.technology import TechnologyCreate, TechnologyResponse

router = APIRouter()


@router.post("/", response_model=TechnologyResponse, status_code=status.HTTP_201_CREATED)
async def create_technology(
    tech_in: TechnologyCreate,
    db: AsyncSession = Depends(get_db),
):
    new_tech = Technology(**tech_in.model_dump())
    db.add(new_tech)

    try:
        await db.commit()
        await db.refresh(new_tech)
        return new_tech
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A technology with this canonical_name or slug already exists.",
        )


@router.get("/", response_model=list[TechnologyResponse])
async def list_technologies(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Technology))
    return result.scalars().all()


@router.get("/{technology_id}", response_model=TechnologyResponse)
async def get_technology(
    technology_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Technology).filter(Technology.id == technology_id)
    )
    tech = result.scalar_one_or_none()

    if not tech:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Technology not found.",
        )

    return tech


@router.get("/{technology_id}/signal", response_model=TechnologySignalResponse)
async def get_technology_signal(
    technology_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    tech_result = await db.execute(
        select(Technology).filter(Technology.id == technology_id)
    )
    tech = tech_result.scalar_one_or_none()

    if not tech:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Technology not found.",
        )

    snapshot_result = await db.execute(
        select(SignalSnapshot)
        .filter(SignalSnapshot.technology_id == technology_id)
        .order_by(
            SignalSnapshot.window_end.desc(),
            SignalSnapshot.computed_at.desc(),
        )
        .limit(1)
    )
    snapshot = snapshot_result.scalar_one_or_none()

    if not snapshot:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No signal snapshot found for this technology.",
        )

    return TechnologySignalResponse(
        technology_id=tech.id,
        canonical_name=tech.canonical_name,
        final_score=snapshot.final_score,
        formula_version=snapshot.formula_version,
        raw_features=snapshot.raw_features,
        component_scores=snapshot.component_scores,
        window_start=snapshot.window_start,
        window_end=snapshot.window_end,
        computed_at=snapshot.computed_at,
    )