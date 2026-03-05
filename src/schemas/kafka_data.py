from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

from src.database.models import ProcessingStatus


class NewUser(BaseModel):
    user_id: int
    username: str
    full_name: str
    created_at: datetime


class NewResume(BaseModel):
    resume_id: int
    user_id: int
    requirement_id: int
    resume: str


class NewRequirement(BaseModel):
    requirement_id: int
    user_id: int
    requirement: str


class NewProcessing(BaseModel):
    processing_id: int
    resume_id: int
    requirement_id: int
    user_id: int


class EndProcessingReceived(BaseModel):
    processing_id: int
    success: Optional[bool] = None
    message_error: Optional[str] = None
    wait_seconds: Optional[int] = None
    score: Optional[int] = None
    matches: Optional[List[str]] = None
    recommendation: Optional[str] = None
    verdict: Optional[str] = None


class EndProcessingForFunc(EndProcessingReceived):
    status: Optional[ProcessingStatus] = None,


class DeleteProcessing(BaseModel):
    processing_ids: List[int]
    resume_ids: List[int]


class DeleteResume(BaseModel):
    resume_ids: List[int]
    processing_ids: List[int]
    requirement_ids: List[int]


class DeleteRequirements(BaseModel):
    requirement_ids: List[int]
    resume_ids: List[int]
    processing_ids: List[int]
    user_id: int