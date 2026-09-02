import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.core.database import get_db
from app.models.technology import Technology
from app.schemas.technology import TechnologyCreate, TechnologyResponse

router = APIRouter()

@router.post("/", response_model=TechnologyResponse, status_code=status.HTTP_201_CREATED)
async def create_technology(tech_in: TechnologyCreate, db: AsyncSession = Depends(get_db)):
    # tech_in is already validated by Pydantic at this point
    new_tech = Technology(**tech_in.model_dump())
    db.add(new_tech)
    
    try:
        await db.commit()
        await db.refresh(new_tech)
        return new_tech
    except IntegrityError:
        await db.rollback()
        # 409 Conflict is the standard HTTP status for duplicate resources
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A technology with this canonical_name or slug already exists."
        )

@router.get("/", response_model=list[TechnologyResponse])
async def list_technologies(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Technology))
    return result.scalars().all()

@router.get("/{technology_id}", response_model=TechnologyResponse)
async def get_technology(technology_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Technology).filter(Technology.id == technology_id))
    tech = result.scalar_one_or_none()
    
    if not tech:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Technology not found."
        )
        
    return tech