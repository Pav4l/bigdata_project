import logging
from sqlalchemy import create_engine, Column, Integer, Text, Numeric, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
    def __init__(self, database_url):
        self.database_url = database_url

    @classmethod
    def from_crawler(cls, crawler):
        return cls(database_url=crawler.settings.get("DATABASE_URL"))

    def open_spider(self, spider):
        self.engine = create_engine(self.database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        logger.info("Connected to database")

    def process_item(self, item, spider):
        """Сохраняем или обновляем данные в БД."""
        session = self.Session()
        try:
            obj = Product(**item)
            session.merge(obj)  # upsert
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving item: {e}")
        finally:
            session.close()
        return item
