"""Assignment API routes."""
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.dependencies import get_current_active_user
from app.db.models import Assignment, Load, LoadDirection, Ramp, Status, User
from app.db.session import get_db
from app.schemas.assignment import (
    AssignmentCreate,
    AssignmentDetailResponse,
    AssignmentResponse,
    AssignmentUpdate,
    ConflictError,
)
from app.services.audit import AuditService
from app.ws.manager import manager

router = APIRouter(prefix="/assignments", tags=["assignments"])


@router.get("/", response_model=List[AssignmentDetailResponse])
async def list_assignments(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    direction: Optional[LoadDirection] = Query(None, description="Filter by load direction"),
    skip: int = 0,
    limit: int = 100,
) -> List[AssignmentDetailResponse]:
    """List all assignments with full details."""
    query = (
        select(Assignment)
        .options(
            selectinload(Assignment.ramp),
            selectinload(Assignment.load),
            selectinload(Assignment.status),
            selectinload(Assignment.creator),
            selectinload(Assignment.updater),
        )
        .join(Load)
    )

    if direction:
        query = query.where(Load.direction == direction)

    result = await db.execute(query.offset(skip).limit(limit))
    assignments = result.scalars().all()
    return [AssignmentDetailResponse.model_validate(assignment) for assignment in assignments]


@router.post("/", response_model=AssignmentDetailResponse, status_code=status.HTTP_201_CREATED)
async def create_assignment(
    assignment_in: AssignmentCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> AssignmentDetailResponse:
    """Create a new assignment."""
    # Validate that ramp, load, and status exist
    ramp_result = await db.execute(select(Ramp).where(Ramp.id == assignment_in.ramp_id))
    if ramp_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ramp not found")

    load_result = await db.execute(select(Load).where(Load.id == assignment_in.load_id))
    if load_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load not found")

    status_result = await db.execute(select(Status).where(Status.id == assignment_in.status_id))
    if status_result.scalar_one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Status not found")

    # Create assignment
    assignment = Assignment(
        **assignment_in.model_dump(),
        created_by=current_user.id,
        updated_by=current_user.id,
    )
    db.add(assignment)
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="assignment",
        entity_id=assignment.id,
        action="CREATE",
        after=assignment.dict(),
    )

    await db.commit()

    # Fetch with relationships
    result = await db.execute(
        select(Assignment)
        .options(
            selectinload(Assignment.ramp),
            selectinload(Assignment.load),
            selectinload(Assignment.status),
            selectinload(Assignment.creator),
            selectinload(Assignment.updater),
        )
        .where(Assignment.id == assignment.id)
    )
    assignment = result.scalar_one()
    assignment_response = AssignmentDetailResponse.model_validate(assignment)

    # Broadcast WebSocket update
    await manager.broadcast_assignment_update(
        assignment_id=assignment.id,
        action="CREATE",
        user_id=current_user.id,
        user_email=current_user.email,
        assignment_data=assignment_response.model_dump(mode="json"),
    )

    return assignment_response


@router.get("/{assignment_id}", response_model=AssignmentDetailResponse)
async def get_assignment(
    assignment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> AssignmentDetailResponse:
    """Get assignment by ID with full details."""
    result = await db.execute(
        select(Assignment)
        .options(
            selectinload(Assignment.ramp),
            selectinload(Assignment.load),
            selectinload(Assignment.status),
            selectinload(Assignment.creator),
            selectinload(Assignment.updater),
        )
        .where(Assignment.id == assignment_id)
    )
    assignment = result.scalar_one_or_none()
    if assignment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")
    return AssignmentDetailResponse.model_validate(assignment)


@router.patch("/{assignment_id}", response_model=AssignmentDetailResponse)
async def update_assignment(
    assignment_id: int,
    assignment_in: AssignmentUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> AssignmentDetailResponse:
    """
    Update assignment with optimistic locking.

    Requires version number to prevent concurrent update conflicts.
    Returns 409 Conflict if version mismatch detected.
    """
    result = await db.execute(select(Assignment).where(Assignment.id == assignment_id))
    assignment = result.scalar_one_or_none()
    if assignment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")

    # Optimistic locking check
    if assignment.version != assignment_in.version:
        # Fetch current data for conflict response
        current_result = await db.execute(
            select(Assignment)
            .options(
                selectinload(Assignment.ramp),
                selectinload(Assignment.load),
                selectinload(Assignment.status),
                selectinload(Assignment.creator),
                selectinload(Assignment.updater),
            )
            .where(Assignment.id == assignment_id)
        )
        current_assignment = current_result.scalar_one()
        current_data = AssignmentDetailResponse.model_validate(current_assignment).model_dump(
            mode="json"
        )

        # Broadcast conflict notification
        await manager.broadcast_conflict(
            assignment_id=assignment_id,
            current_version=assignment.version,
            attempted_version=assignment_in.version,
            current_data=current_data,
        )

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=ConflictError(
                current_version=assignment.version,
                provided_version=assignment_in.version,
                current_data=current_data,
            ).model_dump(),
        )

    before = assignment.dict()

    # Validate references if they're being updated
    update_data = assignment_in.model_dump(exclude_unset=True, exclude={"version"})

    if "ramp_id" in update_data:
        ramp_result = await db.execute(select(Ramp).where(Ramp.id == update_data["ramp_id"]))
        if ramp_result.scalar_one_or_none() is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Ramp not found")

    if "load_id" in update_data:
        load_result = await db.execute(select(Load).where(Load.id == update_data["load_id"]))
        if load_result.scalar_one_or_none() is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Load not found")

    if "status_id" in update_data:
        status_result = await db.execute(
            select(Status).where(Status.id == update_data["status_id"])
        )
        if status_result.scalar_one_or_none() is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Status not found")

    # Update fields
    for field, value in update_data.items():
        setattr(assignment, field, value)

    assignment.updated_by = current_user.id
    assignment.increment_version()
    await db.flush()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="assignment",
        entity_id=assignment.id,
        action="UPDATE",
        before=before,
        after=assignment.dict(),
    )

    await db.commit()

    # Fetch with relationships
    result = await db.execute(
        select(Assignment)
        .options(
            selectinload(Assignment.ramp),
            selectinload(Assignment.load),
            selectinload(Assignment.status),
            selectinload(Assignment.creator),
            selectinload(Assignment.updater),
        )
        .where(Assignment.id == assignment_id)
    )
    assignment = result.scalar_one()
    assignment_response = AssignmentDetailResponse.model_validate(assignment)

    # Broadcast WebSocket update
    await manager.broadcast_assignment_update(
        assignment_id=assignment.id,
        action="UPDATE",
        user_id=current_user.id,
        user_email=current_user.email,
        assignment_data=assignment_response.model_dump(mode="json"),
    )

    return assignment_response


@router.delete("/{assignment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_assignment(
    assignment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
) -> None:
    """Delete assignment."""
    # Fetch with relationships for WebSocket broadcast
    result = await db.execute(
        select(Assignment)
        .options(
            selectinload(Assignment.ramp),
            selectinload(Assignment.load),
            selectinload(Assignment.status),
            selectinload(Assignment.creator),
            selectinload(Assignment.updater),
        )
        .where(Assignment.id == assignment_id)
    )
    assignment = result.scalar_one_or_none()
    if assignment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Assignment not found")

    # Store assignment data for broadcast
    assignment_data = AssignmentDetailResponse.model_validate(assignment).model_dump(mode="json")
    before = assignment.dict()

    # Audit log
    await AuditService.log_action(
        db=db,
        user_id=current_user.id,
        entity_type="assignment",
        entity_id=assignment.id,
        action="DELETE",
        before=before,
    )

    await db.delete(assignment)
    await db.commit()

    # Broadcast WebSocket update
    await manager.broadcast_assignment_update(
        assignment_id=assignment_id,
        action="DELETE",
        user_id=current_user.id,
        user_email=current_user.email,
        assignment_data=assignment_data,
    )
