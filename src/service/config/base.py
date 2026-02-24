from dotenv import load_dotenv


def init_env() -> None:
    load_dotenv(override=False)