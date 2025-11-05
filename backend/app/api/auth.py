"""Authentication API routes."""
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.limiter import limiter
from app.core.security import create_access_token, verify_password
from app.db.models import User
from app.db.session import get_db
from app.schemas.user import Token, UserLogin

router = APIRouter(
    prefix="/auth",
    tags=["auth"],
    responses={
        401: {"description": "Invalid credentials"},
        429: {"description": "Too many login attempts"},
    },
)
settings = get_settings()


@router.post(
    "/login",
    response_model=Token,
    summary="User Login",
    response_description="JWT access token with bearer type",
    responses={
        200: {
            "description": "Successfully authenticated, JWT token returned",
            "content": {
                "application/json": {
                    "example": {
                        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                        "token_type": "bearer"
                    }
                }
            }
        },
        401: {
            "description": "Invalid credentials - incorrect email or password",
            "content": {
                "application/json": {
                    "example": {"detail": "Incorrect email or password"}
                }
            }
        },
        400: {
            "description": "User account is inactive",
            "content": {
                "application/json": {
                    "example": {"detail": "Inactive user"}
                }
            }
        },
        429: {
            "description": "Rate limit exceeded - too many login attempts",
            "content": {
                "application/json": {
                    "example": {"detail": "Rate limit exceeded: 5 per 1 minute"}
                }
            }
        }
    }
)
@limiter.limit("5/minute")
async def login(
    request: Request, user_login: UserLogin, db: AsyncSession = Depends(get_db)
) -> Token:
    """
    Authenticate user and return JWT access token.

    This endpoint validates user credentials and returns a JWT token for authenticated API access.

    **Security Features:**
    - Rate limited to 5 attempts per minute per IP address to prevent brute-force attacks
    - Passwords are verified using bcrypt hashing
    - Inactive users cannot login

    **Token Usage:**
    - Include the returned token in the Authorization header: `Bearer <access_token>`
    - Token expires after configured duration (default: 24 hours)
    - Token contains user ID, email, and role information
    """
    result = await db.execute(select(User).where(User.email == user_login.email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(user_login.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user",
        )

    access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"user_id": user.id, "email": user.email, "role": user.role.value},
        expires_delta=access_token_expires,
    )

    return Token(access_token=access_token, token_type="bearer")
