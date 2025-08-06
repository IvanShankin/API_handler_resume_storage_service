from pydantic import BaseModel


class ProcessingAndRequirementsID(BaseModel):
    processing_id: int
    requirements_id: int