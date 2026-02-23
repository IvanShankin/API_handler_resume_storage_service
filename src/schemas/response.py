from datetime import datetime
from typing import List

from pydantic import BaseModel


class UserOut(BaseModel):
    user_id: int
    username: str
    full_name: str
    created_at: datetime

class ResumeOut(BaseModel):
    resume_id: int
    user_id: int
    resume: str

class RequirementsOut(BaseModel):
    requirements_id: int
    user_id: int
    requirements: str

class ProcessingOut(BaseModel):
    processing_id: int
    resume_id: int
    requirements_id: int
    user_id: int

    success: bool = False
    message_error: str | None = None
    wait_seconds: str | None = None

    create_at: datetime
    score: int | None = None
    verdict: str | None = None


class ProcessingDetailOut(ProcessingOut):
    resume: str
    requirements: str
    matches: List[str] | None = None
    recommendation: str | None = None
