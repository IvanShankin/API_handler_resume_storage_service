import logging
from pathlib import Path

def setup_logging(log_file: Path, level: int = logging.INFO) -> None:
    """Настройка root logger и добавление FileHandler + StreamHandler"""
    log_file.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()  # root logger
    root_logger.setLevel(level)

    # Проверяем, чтобы не добавлять handlers повторно
    if not any(isinstance(h, logging.FileHandler) for h in root_logger.handlers):
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = True  # чтобы сообщения шли к root
    return logger

