"""Ramp API routes."""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_active_user, get_current_admin_user
from app.db.models import Ramp, User
from app.db.session import get_db
from app.schemas.ramp import RampCreate, RampResponse, RampUpdate
from app.services.audit import AuditService

router = APIRouter(prefix="/ramps", tags=["ramps"])


@router.get("/", response_model=List[RampResponse])
async def list_ramps(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    skip: int = 0,
    limit: int = 100,
) -> List[RampResponse]:
    """List all ramps."""
    result = await db.execute(select(Ramp).offset(skip).limit(limit))
    ramps = result.scalars().all()
    return [RampResponse.model_validate(ramp) for ramp in ramps]


@router.post("/", response_model=RampResponse, status_code=status.HTTP_201_CREATED)
async def create_ramp(
    ramp_in: RampCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> RampResponse:
    """Create a new ramp (admin only)."""
    # Check if code already exists
    result = await db.execute(select(Ramp).where(Ramp.code == ramp_in.code))
    if result.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ramp code already exists",
        )

    ramp = Ramp(**ramp_in.model_dump())
    db.add(ramp)
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="ramp",
        entity_id=ramp.id,
        action="CREATE",
        after=ramp.dict(),
    )

    await db.commit()
    await db.refresh(ramp)
    return RampResponse.model_validate(ramp)


@router.get("/{ramp_id}", response_model=RampResponse)
async def get_ramp(
    ramp_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> RampResponse:
    """Get ramp by ID."""
    result = await db.execute(select(Ramp).where(Ramp.id == ramp_id))
    ramp = result.scalar_one_or_none()
    if ramp is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ramp not found")
    return RampResponse.model_validate(ramp)


@router.patch("/{ramp_id}", response_model=RampResponse)
async def update_ramp(
    ramp_id: int,
    ramp_in: RampUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> RampResponse:
    """Update ramp (admin only)."""
    result = await db.execute(select(Ramp).where(Ramp.id == ramp_id))
    ramp = result.scalar_one_or_none()
    if ramp is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ramp not found")

    before = ramp.dict()

    # Update fields
    update_data = ramp_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(ramp, field, value)

    ramp.increment_version()
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="ramp",
        entity_id=ramp.id,
        action="UPDATE",
        before=before,
        after=ramp.dict(),
    )

    await db.commit()
    await db.refresh(ramp)
    return RampResponse.model_validate(ramp)


@router.delete("/{ramp_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_ramp(
    ramp_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> None:
    """Delete ramp (admin only)."""
    result = await db.execute(select(Ramp).where(Ramp.id == ramp_id))
    ramp = result.scalar_one_or_none()
    if ramp is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ramp not found")

    before = ramp.dict()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="ramp",
        entity_id=ramp.id,
        action="DELETE",
        before=before,
    )

    await db.delete(ramp)
    await db.commit()
