"""Status API routes."""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_active_user, get_current_admin_user
from app.db.models import Status, User
from app.db.session import get_db
from app.schemas.status import StatusCreate, StatusResponse, StatusUpdate
from app.services.audit import AuditService

router = APIRouter(prefix="/statuses", tags=["statuses"])


@router.get("/", response_model=List[StatusResponse])
async def list_statuses(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> List[StatusResponse]:
    """List all statuses ordered by sort_order."""
    result = await db.execute(select(Status).order_by(Status.sort_order))
    statuses = result.scalars().all()
    return [StatusResponse.model_validate(status_obj) for status_obj in statuses]


@router.post("/", response_model=StatusResponse, status_code=status.HTTP_201_CREATED)
async def create_status(
    status_in: StatusCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> StatusResponse:
    """Create a new status (admin only)."""
    # Check if code already exists
    result = await db.execute(select(Status).where(Status.code == status_in.code))
    if result.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Status code already exists",
        )

    status_obj = Status(**status_in.model_dump())
    db.add(status_obj)
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="status",
        entity_id=status_obj.id,
        action="CREATE",
        after=status_obj.dict(),
    )

    await db.commit()
    await db.refresh(status_obj)
    return StatusResponse.model_validate(status_obj)


@router.get("/{status_id}", response_model=StatusResponse)
async def get_status(
    status_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> StatusResponse:
    """Get status by ID."""
    result = await db.execute(select(Status).where(Status.id == status_id))
    status_obj = result.scalar_one_or_none()
    if status_obj is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Status not found")
    return StatusResponse.model_validate(status_obj)


@router.patch("/{status_id}", response_model=StatusResponse)
async def update_status(
    status_id: int,
    status_in: StatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> StatusResponse:
    """Update status (admin only)."""
    result = await db.execute(select(Status).where(Status.id == status_id))
    status_obj = result.scalar_one_or_none()
    if status_obj is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Status not found")

    before = status_obj.dict()

    # Update fields
    update_data = status_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(status_obj, field, value)

    status_obj.increment_version()
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="status",
        entity_id=status_obj.id,
        action="UPDATE",
        before=before,
        after=status_obj.dict(),
    )

    await db.commit()
    await db.refresh(status_obj)
    return StatusResponse.model_validate(status_obj)


@router.delete("/{status_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_status(
    status_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user),
) -> None:
    """Delete status (admin only)."""
    result = await db.execute(select(Status).where(Status.id == status_id))
    status_obj = result.scalar_one_or_none()
    if status_obj is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Status not found")

    before = status_obj.dict()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="status",
        entity_id=status_obj.id,
        action="DELETE",
        before=before,
    )

    await db.delete(status_obj)
    await db.commit()
