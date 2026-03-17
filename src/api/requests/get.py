from typing import List, Optional
from fastapi import APIRouter, Depends, Path, Query

from src.api.depends.dependency_provider import get_current_user
from src.database.models import Users
from src.exeptions.service_exc import ServiceException
from src.exeptions.http_exc import ApiException
from src.schemas.request import ResumeSortField
from src.schemas.response import UserOut, ResumeOut, RequirementsOut, ProcessingOut
from src.service.processing import ProcessingService, get_processing_service
from src.service.requirements import get_requirement_service, RequirementService
from src.service.resumes import ResumeService, get_resume_service
from src.service.users import UserService, get_users_service
from src.service.utils.health import health_check_service


router = APIRouter()


@router.get("/health")
async def health_check():
    try:
        await health_check_service()
    except ServiceException:
        raise ApiException()


@router.get("/me", response_model=UserOut)
async def get_me(
    current_user: Users = Depends(get_current_user),
    user_service: UserService = Depends(get_users_service)
):
    return await user_service.get_me(current_user)


@router.get("/get_resume", response_model=ResumeOut)
async def get_resume(
    resume_id: int,
    current_user: Users = Depends(get_current_user),
    resume_service: ResumeService = Depends(get_resume_service)
):
    return await resume_service.get_resume(resume_id, current_user.user_id)


@router.get("/get_resume_by_requirement", response_model=List[ResumeOut])
async def get_resume(
    requirement_id: int,
    sort: Optional[ResumeSortField] = Query(
        default=None,
        description="Сортировка резюме"
    ),
    current_user: Users = Depends(get_current_user),
    resume_service: ResumeService = Depends(get_resume_service)
):
    return await resume_service.get_resume_by_requirements(
        requirement_id,
        current_user.user_id,
        sort=sort
    )


@router.get("/get_requirement/{requirement_id}", response_model=RequirementsOut)
async def get_requirements(
    requirement_id: int = Path(..., ge=1, description="Указание конкретного требования"),
    current_user: Users = Depends(get_current_user),
    requirement_service: RequirementService = Depends(get_requirement_service)
):
    return await requirement_service.get_requirement(requirement_id, current_user.user_id)


@router.get("/get_requirements", response_model=List[RequirementsOut])
async def get_requirements(
    current_user: Users = Depends(get_current_user),
    requirement_service: RequirementService = Depends(get_requirement_service)
):
    """Вернутся все требования данного пользователя"""
    return await requirement_service.get_requirements_by_user(current_user.user_id)


@router.get('/get_processing_by_resume/{resume_id}', response_model=ProcessingOut)
async def get_processing_by_resume(
    resume_id: int = Path(..., ge=1, description="ID резюме у которого будет браться обработка"),
    current_user: Users = Depends(get_current_user),
    processing_service: ProcessingService = Depends(get_processing_service)
):
    return await processing_service.get_processing_by_resume(resume_id, current_user.user_id)


