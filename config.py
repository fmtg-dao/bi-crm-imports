import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()  # run ONCE, early


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    pool_size: int = 5


def load_mysql_config() -> MySQLConfig:
    missing = [k for k in (
        "MYSQL_HOST",
        "MYSQL_USER",
        "MYSQL_PASSWORD",
        "MYSQL_DATABASE",
    ) if not os.getenv(k)]

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return MySQLConfig(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        pool_size=int(os.getenv("MYSQL_POOL_SIZE", "5")),
    )