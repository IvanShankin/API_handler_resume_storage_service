from datetime import timedelta
import logging
from pathlib import Path

STORAGE_TIME_DATA = timedelta(days=1)
STORAGE_TIME_ALLS_DATA = timedelta(hours=8) # используется для хранения всех данных о результатах обработки у конкретного пользователя

MAX_CHAR_REQUIREMENTS = 5000
MAX_CHAR_RESUME = 15000

MIN_COMMIT_COUNT_KAFKA = 10

KEY_NEW_USER = 'new_user'
KEY_NEW_RESUME = 'new_resume'
KEY_NEW_REQUIREMENTS = 'new_requirements'
KEY_NEW_PROCESSING = 'new_processing'

LOG_DIR = Path("../logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "auth_service.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

