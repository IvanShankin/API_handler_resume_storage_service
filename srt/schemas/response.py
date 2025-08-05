from datetime import datetime
from typing import List

from pydantic import BaseModel, EmailStr


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
    score: int
    matches: List[str]
    recommendation: str
    verdict: str

class ProcessingDetailOut(BaseModel):
    processing_id: int
    resume_id: int
    requirements_id: int
    user_id: int
    score: int
    matches: List[str]
    recommendation: str
    verdict: str
    resume: str
    requirements: str