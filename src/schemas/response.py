from datetime import datetime
from typing import List

from pydantic import BaseModel, ConfigDict

from src.database.models import ProcessingStatus


class UserOut(BaseModel):
    user_id: int
    username: str
    full_name: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ResumeOut(BaseModel):
    resume_id: int
    user_id: int
    requirement_id: int
    resume: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RequirementsOut(BaseModel):
    requirement_id: int
    user_id: int
    requirements: str

    model_config = ConfigDict(from_attributes=True)


class ProcessingOut(BaseModel):
    processing_id: int
    resume_id: int
    requirement_id: int
    user_id: int

    status: ProcessingStatus
    success: bool = False

    message_error: str | None = None
    wait_seconds: str | None = None

    score: int | None = None
    matches: List[str] | None = None
    verdict: str | None = None
    recommendation: str | None = None

    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

