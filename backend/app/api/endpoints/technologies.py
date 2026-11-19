import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.database import get_db
from app.models.normalized_event import NormalizedEvent
from app.models.signal_snapshot import SignalSnapshot
from app.models.source import Source
from app.models.source_event import SourceEvent
from app.models.technology import Technology
from app.schemas.domain_relevance import (
    DomainRelevanceRequest,
    TechnologyDomainRelevanceResponse,
)
from app.schemas.evidence import TechnologyEvidenceResponse
from app.schemas.signal_snapshot import TechnologySignalResponse
from app.schemas.technology import TechnologyCreate, TechnologyResponse
from app.services.domain_relevance import calculate_domain_relevance

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
    result = await db.execute(select(Technology).order_by(Technology.canonical_name))
    return result.scalars().all()


@router.post(
    "/domain-relevance",
    response_model=list[TechnologyDomainRelevanceResponse],
)
async def preview_domain_relevance(
    profile: DomainRelevanceRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Preview Phase 1 domain-only relevance for the active technology catalogue.

    Profile input is supplied with this request and is not persisted. The
    signal score remains a separately computed, persisted SignalSnapshot.
    """
    result = await db.execute(
        select(Technology)
        .where(Technology.status == "active")
        .order_by(Technology.canonical_name)
    )
    technologies = result.scalars().all()

    return [
        TechnologyDomainRelevanceResponse(
            technology_id=technology.id,
            canonical_name=technology.canonical_name,
            domain_tags=technology.domain_tags,
            domain_relevance=calculate_domain_relevance(
                technology_domain_tags=technology.domain_tags,
                target_domains=profile.target_domains,
                interests=profile.interests,
            ),
        )
        for technology in technologies
    ]


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


@router.get(
    "/{technology_id}/evidence",
    response_model=list[TechnologyEvidenceResponse],
)
async def get_technology_evidence(
    technology_id: uuid.UUID,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Return stored evidence and its source identity without recomputing a signal."""
    technology_result = await db.execute(
        select(Technology.id).where(Technology.id == technology_id)
    )
    if technology_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Technology not found.",
        )

    evidence_result = await db.execute(
        select(NormalizedEvent, SourceEvent, Source)
        .join(
            SourceEvent,
            NormalizedEvent.source_event_id == SourceEvent.id,
        )
        .join(
            Source,
            SourceEvent.source_id == Source.id,
        )
        .where(NormalizedEvent.technology_id == technology_id)
        .order_by(
            NormalizedEvent.occurred_at.desc(),
            NormalizedEvent.id.desc(),
        )
        .limit(limit)
        .offset(offset)
    )

    return [
        TechnologyEvidenceResponse(
            normalized_event_id=normalized_event.id,
            source_name=source.name,
            source_type=source.type,
            source_native_id=source_event.source_native_id,
            summary_text=normalized_event.summary_text,
            source_url=normalized_event.source_url,
            occurred_at=normalized_event.occurred_at,
            fetched_at=source_event.fetched_at,
        )
        for normalized_event, source_event, source in evidence_result.all()
    ]
