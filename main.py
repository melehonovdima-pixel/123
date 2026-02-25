# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8') if hasattr(sys.stdout, 'reconfigure') else None

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request as FastAPIRequest
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.orm import Session
from sqlalchemy.orm import joinedload
from sqlalchemy import func, or_
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from database import get_db, init_db
from models import User, Request, Stage, Comment, SystemSettings, UserRole, UserStatus, RequestStatus, RequestType, StageStatus
from sqlalchemy.orm import joinedload
from schemas import (
    UserCreate, UserInDB, UserPublic, UserUpdate, UserUpdateAdmin,
    RequestCreate, RequestUpdate, RequestInDB, RequestWithDetails, RequestWithStages,
    StageCreate, StageUpdate, StageInDB, StageWithExecutor, StageWithRequest,
    RequestBasicInfo,
    CommentCreate, CommentInDB, CommentWithUser,
    LoginRequest, Token,
    SystemSettingUpdate, SystemSettingInDB,
    DashboardStats, ExecutorStats, OverdueRequest, ExecutorReport
)
from auth import (
    authenticate_user, create_access_token, get_password_hash,
    get_current_active_user, require_admin, require_manager, require_executor
)
from config import settings

from fastapi import Request as FastAPIRequest
from fastapi.responses import Response

from starlette.middleware.base import BaseHTTPMiddleware

def to_naive_utc(dt) -> datetime | None:
    """
    Приводит datetime к naive UTC.
    Нужно для совместимости старых (naive) и новых (aware) записей в БД.
    """
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def recalculate_request_status(request: Request, db: Session) -> None:
    """Пересчитывает статус заявки на основе её этапов."""
    stages = db.query(Stage).filter(Stage.request_id == request.id).all()
    if not stages:
        return
    statuses = [s.status for s in stages]
    if all(s == StageStatus.COMPLETED for s in statuses):
        request.status = RequestStatus.COMPLETED
        if not request.completed_at:
            request.completed_at = datetime.utcnow()
    elif any(s in (StageStatus.IN_PROGRESS, StageStatus.COMPLETED) for s in statuses) \
         or any(s.executor_id is not None for s in stages):
        request.status = RequestStatus.IN_PROGRESS
    else:
        request.status = RequestStatus.NEW
    db.commit()


# Create FastAPI app
app = FastAPI(
    title="Система управления заявками",
    description="API для системы управления заявками",
    version="1.0.0"
)


# CORS middleware
origins = [
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://front.moj-proj.netcraze.pro",
    "https://123-snowy-tau-30.vercel.app",  # ← https, не http!
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["Content-Type", "Authorization", "ngrok-skip-browser-warning"],
)





# ==================== Initialization ====================

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_db()
    
    # Create default admin user if not exists
    db = next(get_db())
    try:
        admin = db.query(User).filter(User.username == "1488").first()
        if not admin:
            admin = User(
                username="1488",
                hashed_password=get_password_hash("0000"),
                fullname="Администратор Системы",
                address="Главный офис",
                role=UserRole.ADMIN,
                status=UserStatus.CONFIRMED,
                is_active=True
            )
            db.add(admin)
            db.commit()
            print("✓ Default admin user created (username: 1488, password: 0000)")
        
        # Create default system settings
        setting = db.query(SystemSettings).filter(SystemSettings.key == "response_time_hours").first()
        if not setting:
            setting = SystemSettings(
                key="response_time_hours",
                value="24",
                description="Время ответа на заявку (часы)"
            )
            db.add(setting)
            db.commit()
            print("✓ Default system settings created")
    finally:
        db.close()


# ==================== Health Check ====================

@app.get("/")
async def root():
    """API health check"""
    return {
        "status": "ok",
        "message": "УК ЖКХ API is running",
        "version": "1.0.0"
    }


# ==================== Authentication ====================

@app.post("/api/auth/register", response_model=UserInDB, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """
    Register a new user (client)
    """
    # Check if user already exists
    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь с таким номером телефона уже существует"
        )
    
    # Create new user
    new_user = User(
        username=user_data.username,
        hashed_password=get_password_hash(user_data.password),
        fullname=user_data.fullname,
        address=user_data.address,
        role=UserRole.CLIENT,
        status=UserStatus.CONFIRMED,
        is_active=True
    )
    
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    return new_user


@app.post("/api/auth/login", response_model=Token)
async def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    """
    Login and get JWT token
    """
    user = authenticate_user(db, login_data.username, login_data.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный логин или пароль",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token
    access_token = create_access_token(
        data={"sub": user.username, "user_id": user.id, "role": user.role.value}
    )
    
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/api/auth/me", response_model=UserInDB)
async def get_current_user_info(current_user: User = Depends(get_current_active_user)):
    """
    Get current user information
    """
    return current_user


# ==================== Users ====================

# ==================== Блокировка / Разблокировка ====================

@app.put("/api/users/{user_id}/block", response_model=UserInDB)
async def block_user(
    user_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Заблокировать учётную запись пользователя (только администратор).
    Устанавливает is_active=False и status=BLOCKED.
    """
    # Администратор не может заблокировать самого себя
    if current_user.id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Администратор не может заблокировать собственную учётную запись"
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )

    # Нельзя заблокировать другого администратора
    if user.role == UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Невозможно заблокировать учётную запись администратора"
        )

    # Проверяем, не заблокирован ли уже
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь уже заблокирован"
        )

    user.is_active = False
    user.status = UserStatus.BLOCKED
    db.commit()
    db.refresh(user)
    return user


@app.put("/api/users/{user_id}/unblock", response_model=UserInDB)
async def unblock_user(
    user_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Разблокировать учётную запись пользователя (только администратор).
    Устанавливает is_active=True и status=CONFIRMED.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )

    # Проверяем, действительно ли пользователь заблокирован
    if user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пользователь не заблокирован"
        )

    user.is_active = True
    user.status = UserStatus.CONFIRMED
    db.commit()
    db.refresh(user)
    return user


@app.get("/api/users", response_model=List[UserInDB])
async def get_users(
    skip: int = 0,
    limit: int = 100,
    role: Optional[UserRole] = None,
    status: Optional[UserStatus] = None,
    search: Optional[str] = None,
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    """
    Get list of users (managers and admins only)
    """
    query = db.query(User)
    
    # Filter by role
    if role:
        query = query.filter(User.role == role)
    
    # Filter by status
    if status:
        query = query.filter(User.status == status)
    
    # Search by username or fullname
    if search:
        query = query.filter(
            or_(
                User.username.ilike(f"%{search}%"),
                User.fullname.ilike(f"%{search}%")
            )
        )
    
    users = query.offset(skip).limit(limit).all()
    return users


@app.get("/api/users/{user_id}", response_model=UserInDB)
async def get_user(
    user_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get user by ID
    """
    # Users can view their own profile, managers can view all
    if current_user.role not in [UserRole.ADMIN, UserRole.MANAGER] and current_user.id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Необходима авторизация для просмотра профиля пользователя"
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )
    
    return user


@app.put("/api/users/{user_id}", response_model=UserInDB)
async def update_user(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update user information
    """
    # Users can update their own profile
    if current_user.id != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет прав для изменения профиля пользователя"
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )
    
    # Update fields
    if user_update.fullname:
        user.fullname = user_update.fullname
    if user_update.address:
        user.address = user_update.address
    if user_update.password:
        user.hashed_password = get_password_hash(user_update.password)
    
    db.commit()
    db.refresh(user)
    
    return user


@app.put("/api/users/{user_id}/admin", response_model=UserInDB)
async def update_user_admin(
    user_id: int,
    user_update: UserUpdateAdmin,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Update user information (admin only)
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )
    
    # Update fields
    if user_update.fullname:
        user.fullname = user_update.fullname
    if user_update.address:
        user.address = user_update.address
    if user_update.password:
        user.hashed_password = get_password_hash(user_update.password)
    if user_update.role:
        user.role = user_update.role
    if user_update.status:
        user.status = user_update.status
    if user_update.is_active is not None:
        user.is_active = user_update.is_active
    
    db.commit()
    db.refresh(user)
    
    return user


@app.delete("/api/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Delete user (admin only)
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Пользователь не найден"
        )
    
    db.delete(user)
    db.commit()
    
    return None


# ==================== Requests ====================

@app.get("/api/requests", response_model=List[RequestWithDetails])
async def get_requests(
    skip: int = 0,
    limit: int = 100,
    status_filter: Optional[RequestStatus] = None,
    type_filter: Optional[RequestType] = None,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    query = db.query(Request).options(joinedload(Request.client))

    if current_user.role == UserRole.CLIENT:
        query = query.filter(Request.client_id == current_user.id)
    elif current_user.role == UserRole.EXECUTOR:
        # Исполнитель видит заявки, в которых у него есть этапы
        ids = [r[0] for r in db.query(Stage.request_id).filter(
            Stage.executor_id == current_user.id
        ).distinct().all()]
        query = query.filter(Request.id.in_(ids))

    if status_filter:
        query = query.filter(Request.status == status_filter)
    if type_filter:
        query = query.filter(Request.type == type_filter)

    return query.order_by(Request.created_at.desc()).offset(skip).limit(limit).all()

@app.get("/api/requests/{request_id}/with-stages", response_model=RequestWithStages)
async def get_request_with_stages(
    request_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    request = db.query(Request).options(
        joinedload(Request.client),
        joinedload(Request.stages).joinedload(Stage.executor)
    ).filter(Request.id == request_id).first()
    if not request:
        raise HTTPException(status_code=404, detail="Request not found")
    if current_user.role == UserRole.CLIENT and request.client_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    if current_user.role == UserRole.EXECUTOR:
        has_stage = any(s.executor_id == current_user.id for s in request.stages)
        if not has_stage:
            raise HTTPException(status_code=403, detail="Access denied")
    return request


# ==================== Просроченные заявки ====================

@app.get("/api/requests/overdue", response_model=List[OverdueRequest])
async def get_overdue_requests(
    hours_threshold: int = 48,
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    now_py = datetime.utcnow()  # naive, для Python-арифметики
    threshold_dt = now_py + timedelta(hours=hours_threshold)

    requests = db.query(Request).options(
    joinedload(Request.client)
).filter(
    Request.deadline.isnot(None),
    Request.deadline <= threshold_dt,
    Request.status.notin_([
        RequestStatus.COMPLETED,
        RequestStatus.CANCELLED
    ])
).order_by(Request.deadline.asc()).all()

    result = []
    for req in requests:
        # Нормализуем дедлайн к naive для арифметики
        dl = to_naive_utc(req.deadline)
        diff_hours = round((dl - now_py).total_seconds() / 3600, 1)

        if diff_hours < 0:
            urgency = "overdue"
        elif diff_hours < 24:
            urgency = "today"
        else:
            urgency = "soon"

        result.append(OverdueRequest(
            id=req.id,
            type=req.type,
            description=req.description,
            status=req.status,
            deadline=req.deadline,
            client=req.client,
            hours_overdue=diff_hours,
            urgency=urgency
        ))

    return result

@app.get("/api/requests/{request_id}", response_model=RequestWithDetails)
async def get_request(
    request_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get request by ID
    """
    request = db.query(Request).filter(Request.id == request_id).first()
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена"
        )
    
    # Check access rights
    if current_user.role == UserRole.CLIENT and request.client_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет доступа к этой заявке"
        )
    
    if current_user.role == UserRole.EXECUTOR:
        has_stage = db.query(Stage).filter(
            Stage.request_id == request_id,
            Stage.executor_id == current_user.id
        ).first()
        if not has_stage:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет доступа к этой заявке"
            )
    
    return request


@app.post("/api/requests", response_model=RequestInDB, status_code=status.HTTP_201_CREATED)
async def create_request(
    request_data: RequestCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    response_time_setting = db.query(SystemSettings).filter(
        SystemSettings.key == "response_time_hours"
    ).first()
    response_hours = int(response_time_setting.value) if response_time_setting else 24
    deadline = datetime.utcnow() + timedelta(hours=response_hours)

    new_request = Request(
        client_id=current_user.id,
        type=request_data.type,       # может быть None
        description=request_data.description,
        status=RequestStatus.NEW,
        priority=1,
        deadline=deadline
    )
    db.add(new_request)
    db.commit()
    db.refresh(new_request)
    return new_request





@app.put("/api/requests/{request_id}", response_model=RequestInDB)
async def update_request(
    request_id: int,
    request_update: RequestUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Update request
    """
    request = db.query(Request).filter(Request.id == request_id).first()
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена"
        )
    
    # Check permissions
    is_owner = request.client_id == current_user.id
    is_manager = current_user.role in [UserRole.ADMIN, UserRole.MANAGER]
    
    if not (is_owner or is_manager):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет прав для изменения этой заявки"
        )
    
    # Update fields
    if request_update.description is not None and is_owner:
        request.description = request_update.description
    
    if request_update.status is not None:
        request.status = request_update.status
        if request_update.status == RequestStatus.COMPLETED and not request.completed_at:
            request.completed_at = datetime.now(timezone.utc)
    
    if request_update.priority is not None and is_manager:
        request.priority = request_update.priority

    if request_update.type is not None and is_manager:
        request.type = request_update.type

    if request_update.deadline is not None and is_manager:
        request.deadline = request_update.deadline
    
    db.commit()
    db.refresh(request)
    
    return request


@app.delete("/api/requests/{request_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_request(
    request_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete request
    """
    request = db.query(Request).filter(Request.id == request_id).first()
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена"
        )
    
    # Only client who created or admin can delete
    if request.client_id != current_user.id and current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет прав для удаления этой заявки"
        )
    
    db.delete(request)
    db.commit()
    
    return None


# ==================== Comments ====================

@app.get("/api/requests/{request_id}/comments", response_model=List[CommentWithUser])
async def get_request_comments(
    request_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get comments for a request
    """
    # Check if request exists and user has access
    request = db.query(Request).filter(Request.id == request_id).first()
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена"
        )
    
    # Check access rights
    if current_user.role == UserRole.CLIENT and request.client_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет доступа к комментариям этой заявки"
        )
    
    comments = db.query(Comment).filter(Comment.request_id == request_id).order_by(Comment.created_at).all()
    return comments


@app.post("/api/comments", response_model=CommentInDB, status_code=status.HTTP_201_CREATED)
async def create_comment(
    comment_data: CommentCreate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Create a comment on a request
    """
    # Check if request exists
    request = db.query(Request).filter(Request.id == comment_data.request_id).first()
    if not request:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Заявка не найдена"
        )
    
    # Check access rights
    if current_user.role == UserRole.CLIENT and request.client_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Нет прав для добавления комментария к этой заявке"
        )
    
    if current_user.role == UserRole.EXECUTOR:
        has_stage = db.query(Stage).filter(
            Stage.request_id == request.id,
            Stage.executor_id == current_user.id
        ).first()
        if not has_stage:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Нет прав для добавления комментария к этой заявке"
            )
    
    new_comment = Comment(
        request_id=comment_data.request_id,
        user_id=current_user.id,
        text=comment_data.text
    )
    
    db.add(new_comment)
    db.commit()
    db.refresh(new_comment)
    
    return new_comment

# ==================== Stages ====================

@app.get("/api/stages/my", response_model=List[StageWithRequest])
async def get_my_stages(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Этапы, назначенные текущему исполнителю."""
    return db.query(Stage).options(
        joinedload(Stage.executor),
        joinedload(Stage.request).joinedload(Request.client)
    ).filter(
        Stage.executor_id == current_user.id,
        Stage.status != StageStatus.COMPLETED
    ).order_by(Stage.deadline.asc()).all()

@app.get("/api/requests/{request_id}/stages", response_model=List[StageWithExecutor])
async def get_request_stages(
    request_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    req = db.query(Request).filter(Request.id == request_id).first()
    if not req:
        raise HTTPException(status_code=404, detail="Not found")
    if current_user.role == UserRole.CLIENT and req.client_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")
    return db.query(Stage).options(joinedload(Stage.executor)).filter(
        Stage.request_id == request_id
    ).order_by(Stage.order_num).all()

@app.post("/api/requests/{request_id}/stages", response_model=StageInDB, status_code=201)
async def create_stage(
    request_id: int,
    stage_data: StageCreate,
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    req = db.query(Request).filter(Request.id == request_id).first()
    if not req:
        raise HTTPException(status_code=404, detail="Request not found")
    if stage_data.executor_id:
        ex = db.query(User).filter(User.id == stage_data.executor_id).first()
        if not ex or ex.role != UserRole.EXECUTOR:
            raise HTTPException(status_code=400, detail="Invalid executor")
    max_order = db.query(func.max(Stage.order_num)).filter(
        Stage.request_id == request_id
    ).scalar() or 0
    order_num = stage_data.order_num if stage_data.order_num else max_order + 1
    new_stage = Stage(
        request_id=request_id,
        executor_id=stage_data.executor_id,
        order_num=order_num,
        description=stage_data.description,
        deadline=stage_data.deadline,
        status=StageStatus.PENDING
    )
    db.add(new_stage)
    db.commit()
    db.refresh(new_stage)
    recalculate_request_status(req, db)
    return new_stage

@app.put("/api/stages/{stage_id}", response_model=StageInDB)
async def update_stage(
    stage_id: int,
    stage_update: StageUpdate,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    is_manager = current_user.role in (UserRole.MANAGER, UserRole.ADMIN)
    is_own_executor = (current_user.role == UserRole.EXECUTOR and
                       stage.executor_id == current_user.id)
    if not (is_manager or is_own_executor):
        raise HTTPException(status_code=403, detail="Access denied")
    if is_manager:
        if stage_update.description is not None: stage.description = stage_update.description
        if stage_update.executor_id is not None: stage.executor_id = stage_update.executor_id
        if stage_update.deadline is not None:    stage.deadline = stage_update.deadline
        if stage_update.order_num is not None:   stage.order_num = stage_update.order_num
    if stage_update.status is not None:
        stage.status = stage_update.status
        if stage_update.status == StageStatus.COMPLETED and not stage.completed_at:
            stage.completed_at = datetime.utcnow()
    db.commit()
    req = db.query(Request).filter(Request.id == stage.request_id).first()
    if req:
        recalculate_request_status(req, db)
    db.refresh(stage)
    return stage

@app.delete("/api/stages/{stage_id}", status_code=204)
async def delete_stage(
    stage_id: int,
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    request_id = stage.request_id
    db.delete(stage)
    db.commit()
    req = db.query(Request).filter(Request.id == request_id).first()
    if req:
        recalculate_request_status(req, db)
    return None

@app.post("/api/stages/{stage_id}/start", response_model=StageInDB)
async def start_stage(
    stage_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    if stage.executor_id != current_user.id and current_user.role not in (UserRole.MANAGER, UserRole.ADMIN):
        raise HTTPException(status_code=403, detail="Access denied")
    stage.status = StageStatus.IN_PROGRESS
    db.commit()
    req = db.query(Request).filter(Request.id == stage.request_id).first()
    if req:
        recalculate_request_status(req, db)
    db.refresh(stage)
    return stage

@app.post("/api/stages/{stage_id}/complete", response_model=StageInDB)
async def complete_stage(
    stage_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    stage = db.query(Stage).filter(Stage.id == stage_id).first()
    if not stage:
        raise HTTPException(status_code=404, detail="Stage not found")
    if stage.executor_id != current_user.id and current_user.role not in (UserRole.MANAGER, UserRole.ADMIN):
        raise HTTPException(status_code=403, detail="Access denied")
    stage.status = StageStatus.COMPLETED
    stage.completed_at = datetime.utcnow()
    db.commit()
    req = db.query(Request).filter(Request.id == stage.request_id).first()
    if req:
        recalculate_request_status(req, db)
    db.refresh(stage)
    return stage


# ==================== Statistics ====================

@app.get("/api/stats/dashboard", response_model=DashboardStats)
async def get_dashboard_stats(
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    """
    Get dashboard statistics (managers and admins only)
    """
    total_requests = db.query(func.count(Request.id)).scalar()
    new_requests = db.query(func.count(Request.id)).filter(Request.status == RequestStatus.NEW).scalar()
    in_progress_requests = db.query(func.count(Request.id)).filter(
        Request.status == RequestStatus.IN_PROGRESS
    ).scalar()
    completed_requests = db.query(func.count(Request.id)).filter(Request.status == RequestStatus.COMPLETED).scalar()
    
    total_users = db.query(func.count(User.id)).scalar()
    total_clients = db.query(func.count(User.id)).filter(User.role == UserRole.CLIENT).scalar()
    total_executors = db.query(func.count(User.id)).filter(User.role == UserRole.EXECUTOR).scalar()
    
    return DashboardStats(
        total_requests=total_requests,
        new_requests=new_requests,
        in_progress_requests=in_progress_requests,
        completed_requests=completed_requests,
        total_users=total_users,
        total_clients=total_clients,
        total_executors=total_executors
    )

@app.get("/api/stats/executor", response_model=ExecutorStats)
async def get_executor_stats(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    executor_id = current_user.id
    now = datetime.utcnow()
    month_start = datetime(now.year, now.month, 1)

    completed_this_month = db.query(func.count(Stage.id)).filter(
        Stage.executor_id == executor_id,
        Stage.status == StageStatus.COMPLETED,
        Stage.completed_at >= month_start
    ).scalar() or 0

    in_progress_now = db.query(func.count(Stage.id)).filter(
        Stage.executor_id == executor_id,
        Stage.status.in_([StageStatus.PENDING, StageStatus.IN_PROGRESS])
    ).scalar() or 0

    total_completed = db.query(func.count(Stage.id)).filter(
        Stage.executor_id == executor_id,
        Stage.status == StageStatus.COMPLETED
    ).scalar() or 0

    total_assigned = db.query(func.count(Stage.id)).filter(
        Stage.executor_id == executor_id
    ).scalar() or 0

    overdue_now = db.query(func.count(Stage.id)).filter(
        Stage.executor_id == executor_id,
        Stage.deadline.isnot(None),
        Stage.deadline < func.now(),
        Stage.status != StageStatus.COMPLETED
    ).scalar() or 0

    return ExecutorStats(
        completed_this_month=completed_this_month,
        in_progress_now=in_progress_now,
        total_completed=total_completed,
        total_assigned=total_assigned,
        overdue_now=overdue_now
    )







# ==================== Отчёт по исполнителям ====================

@app.get("/api/stats/executors-report", response_model=List[ExecutorReport])
async def get_executors_report(
    current_user: User = Depends(require_manager),
    db: Session = Depends(get_db)
):
    executors = db.query(User).filter(
        User.role == UserRole.EXECUTOR, User.is_active == True
    ).all()
    result = []
    for ex in executors:
        total_assigned  = db.query(func.count(Stage.id)).filter(Stage.executor_id == ex.id).scalar() or 0
        total_completed = db.query(func.count(Stage.id)).filter(
            Stage.executor_id == ex.id, Stage.status == StageStatus.COMPLETED
        ).scalar() or 0
        overdue_count = db.query(func.count(Stage.id)).filter(
            Stage.executor_id == ex.id,
            Stage.deadline.isnot(None),
            Stage.deadline < func.now(),
            Stage.status != StageStatus.COMPLETED
        ).scalar() or 0
        done_stages = db.query(Stage).filter(
            Stage.executor_id == ex.id,
            Stage.status == StageStatus.COMPLETED,
            Stage.completed_at.isnot(None),
            Stage.created_at.isnot(None)
        ).all()
        avg_hours = None
        if done_stages:
            deltas = []
            for s in done_stages:
                start = to_naive_utc(s.created_at)
                end   = to_naive_utc(s.completed_at)
                if start and end and end > start:
                    deltas.append((end - start).total_seconds())
            if deltas:
                avg_hours = round(sum(deltas) / 3600 / len(deltas), 1)
        completion_rate = round(
            (total_completed / total_assigned * 100) if total_assigned > 0 else 0, 1
        )
        result.append(ExecutorReport(
            executor_id=ex.id, fullname=ex.fullname, username=ex.username,
            total_assigned=total_assigned, total_completed=total_completed,
            overdue_count=overdue_count, avg_hours=avg_hours,
            completion_rate=completion_rate
        ))
    result.sort(key=lambda x: x.completion_rate, reverse=True)
    return result





# ==================== System Settings ====================

@app.get("/api/settings", response_model=List[SystemSettingInDB])
async def get_settings(
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Get all system settings (admins only)
    """
    settings = db.query(SystemSettings).all()
    return settings


@app.put("/api/settings/{setting_key}", response_model=SystemSettingInDB)
async def update_setting(
    setting_key: str,
    setting_update: SystemSettingUpdate,
    current_user: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    Update system setting (admins only)
    """
    setting = db.query(SystemSettings).filter(SystemSettings.key == setting_key).first()
    if not setting:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Настройка не найдена"
        )
    
    setting.value = setting_update.value
    
    db.commit()
    db.refresh(setting)
    
    return setting


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )

