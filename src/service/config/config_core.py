from typing import Optional

from src.service.config.schemas import Config

_config: Optional[Config] = None


def init_config() -> Config:
    new_conf = Config()
    set_config(new_conf)
    return new_conf


def set_config(conf: Config):
    global _config
    _config = conf


def get_config() -> Config:
    global _config
    if _config is None:
        raise RuntimeError("Config not initialized")
    return _config