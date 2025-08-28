# вспомогательные функции для search_processing
from src.database.models import Processing


async def prepare_processing_data(p: Processing) -> dict:
    """Формирует словарь из Processing, с такими же ключами как поля у Processing"""
    return {
        "processing_id": p.processing_id,
        "resume_id": p.resume_id,
        "requirements_id": p.requirements_id,
        "user_id": p.user_id,
        "create_at": p.create_at.strftime("%Y-%m-%d %H:%M:%S%z"),
        "score": p.score,
        "matches": p.matches,
        "recommendation": p.recommendation,
        "verdict": p.verdict,
        "resume": p.resume.resume if p.resume else None,
        "requirements": p.requirements.requirements if p.requirements else None
    }