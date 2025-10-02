import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker
from src.utils.config import ENV_PATH

load_dotenv(ENV_PATH)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


class Database:
    TRADING_URL = URL.create(
        "mysql+pymysql",
        username=DB_USER,
        password=DB_PASSWORD,
        host="127.0.0.1",
        port=3306,
        database="trading",
        query={"charset": "utf8mb4"},
    )

    def __init__(
        self,
        url: str | URL | None = None,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_pre_ping: bool = True,
        echo: bool = False,
    ):
        if url is None:
            url = self.TRADING_URL
        self.engine = create_engine(
            url=url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=False,
            future=True,
        )
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
        )

    @contextmanager
    def session(self):
        s = self.SessionLocal()
        try:
            yield s
            s.commit()

        except:
            s.rollback()

        finally:
            s.close()
