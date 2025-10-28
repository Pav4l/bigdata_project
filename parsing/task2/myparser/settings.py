import os

BOT_NAME = "myparser"

SPIDER_MODULES = ["myparser.spiders"]
NEWSPIDER_MODULE = "myparser.spiders"

# --- Настройки базы данных PostgreSQL ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@db:5432/myparser_db")

# --- Подключаем middleware ---
DOWNLOADER_MIDDLEWARES = {
    "myparser.middlewares.RotateUserAgentMiddleware": 100,
    "myparser.middlewares.SeleniumMiddleware": 800,
}

# --- Подключаем pipeline ---
ITEM_PIPELINES = {
    "myparser.pipelines.MyparserPipeline": 300,
}

# --- Настройки Selenium ---
SELENIUM_TIMEOUT = 20
SELENIUM_HEADLESS = True

# --- Общие параметры ---
ROBOTSTXT_OBEY = False
LOG_LEVEL = "INFO"
DOWNLOAD_DELAY = 1
CONCURRENT_REQUESTS = 8
