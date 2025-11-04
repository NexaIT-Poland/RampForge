"""Load API routes."""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_active_user
from app.db.models import Load, LoadDirection, User
from app.db.session import get_db
from app.schemas.load import LoadCreate, LoadResponse, LoadUpdate
from app.services.audit import AuditService

router = APIRouter(prefix="/loads", tags=["loads"])


@router.get("/", response_model=List[LoadResponse])
async def list_loads(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    direction: Optional[LoadDirection] = Query(None, description="Filter by direction (IB/OB)"),
    skip: int = 0,
    limit: int = 100,
) -> List[LoadResponse]:
    """List all loads with optional direction filter."""
    query = select(Load)
    if direction:
        query = query.where(Load.direction == direction)
    result = await db.execute(query.offset(skip).limit(limit))
    loads = result.scalars().all()
    return [LoadResponse.model_validate(load) for load in loads]


@router.post("/", response_model=LoadResponse, status_code=status.HTTP_201_CREATED)
async def create_load(
    load_in: LoadCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> LoadResponse:
    """Create a new load."""
    # Check if reference already exists
    result = await db.execute(select(Load).where(Load.reference == load_in.reference))
    if result.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Load reference already exists",
        )

    load = Load(**load_in.model_dump())
    db.add(load)
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="load",
        entity_id=load.id,
        action="CREATE",
        after=load.dict(),
    )

    await db.commit()
    await db.refresh(load)
    return LoadResponse.model_validate(load)


@router.get("/{load_id}", response_model=LoadResponse)
async def get_load(
    load_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> LoadResponse:
    """Get load by ID."""
    result = await db.execute(select(Load).where(Load.id == load_id))
    load = result.scalar_one_or_none()
    if load is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load not found")
    return LoadResponse.model_validate(load)


@router.patch("/{load_id}", response_model=LoadResponse)
async def update_load(
    load_id: int,
    load_in: LoadUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> LoadResponse:
    """Update load."""
    result = await db.execute(select(Load).where(Load.id == load_id))
    load = result.scalar_one_or_none()
    if load is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load not found")

    before = load.dict()

    # Update fields
    update_data = load_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(load, field, value)

    load.increment_version()
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="load",
        entity_id=load.id,
        action="UPDATE",
        before=before,
        after=load.dict(),
    )

    await db.commit()
    await db.refresh(load)
    return LoadResponse.model_validate(load)


@router.delete("/{load_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_load(
    load_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> None:
    """Delete load."""
    result = await db.execute(select(Load).where(Load.id == load_id))
    load = result.scalar_one_or_none()
    if load is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load not found")

    before = load.dict()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="load",
        entity_id=load.id,
        action="DELETE",
        before=before,
    )

    await db.delete(load)
    await db.commit()
