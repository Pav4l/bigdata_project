import logging
import os
import time
from sqlalchemy import create_engine, Column, Integer, Text, Numeric, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)
Base = declarative_base()


class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    url = Column(Text, unique=True)
    parse_time = Column(DateTime)
    title = Column(Text)
    description = Column(Text)
    price = Column(Numeric(10, 2))
    attributes = Column(JSON)
    comments = Column(JSON)
    links = Column(JSON)


class MyparserPipeline:
    """Пайплайн для сохранения данных в PostgreSQL."""

    def __init__(self, database_url=None):
        # Загружаем URL из переменных окружения, если не передан
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL is not set in environment variables")

    @classmethod
    def from_crawler(cls, crawler):
        """Создание пайплайна через Scrapy settings."""
        return cls(database_url=crawler.settings.get("DATABASE_URL"))

    def open_spider(self, spider):
        """Подключение к БД с повторными попытками (если Postgres ещё не готов)."""
        max_retries = 10
        delay = 3

        for attempt in range(1, max_retries + 1):
            try:
                self.engine = create_engine(self.database_url)
                # Проверим соединение
                conn = self.engine.connect()
                conn.close()

                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
                logger.info("✅ Successfully connected to PostgreSQL")
                break
            except OperationalError as e:
                logger.warning(f"⏳ Database not ready (attempt {attempt}/{max_retries}): {e}")
                time.sleep(delay)
        else:
            raise ConnectionError("❌ Could not connect to PostgreSQL after several attempts")

    def process_item(self, item, spider):
        """Сохраняем или обновляем данные в БД."""
        session = self.Session()
        try:
            obj = Product(**item)
            session.merge(obj)  # upsert
            session.commit()
            logger.debug(f"Saved item: {obj.url}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving item: {e}")
        finally:
            session.close()
        return item
