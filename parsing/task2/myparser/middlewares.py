import random
import time
import logging
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

# --- Список случайных User-Agent ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]


class RotateUserAgentMiddleware:
    """Меняет User-Agent на каждом запросе для имитации разных клиентов."""
    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(USER_AGENTS)


class SeleniumMiddleware:
    """Использует Selenium для обработки динамических страниц."""

    def __init__(self, timeout=30):
        self.timeout = timeout
        self.driver = None  # создадим лениво при первом запросе

    @classmethod
    def from_crawler(cls, crawler):
        timeout = crawler.settings.getint("SELENIUM_TIMEOUT", 30)
        return cls(timeout=timeout)

    def _ensure_driver(self):
        """Создание драйвера только при первом использовании."""
        if self.driver:
            return

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        # важно: подключаемся к удалённому Selenium (в docker-сервисе)
        self.driver = webdriver.Remote(
            command_executor="http://selenium-chrome:4444/wd/hub",
            options=chrome_options
        )
        self.driver.set_page_load_timeout(self.timeout)
        self.wait = WebDriverWait(self.driver, self.timeout)
        logger.info("Selenium Remote WebDriver initialized")

    def process_request(self, request, spider):
        """Если meta содержит use_selenium=True — используем браузер."""
        if not request.meta.get("use_selenium"):
            return None

        self._ensure_driver()
        spider.logger.info(f"Selenium-загрузка: {request.url}")

        try:
            self.driver.get(request.url)
        except Exception as e:
            spider.logger.warning(f"Selenium error while loading {request.url}: {e}")
            return None

        # ожидание элемента, если указан
        wait_css = request.meta.get("wait_for_css")
        if wait_css:
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))
            except Exception:
                logger.warning(f"Timeout waiting for selector: {wait_css}")

        # простая прокрутка вниз
        try:
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except Exception:
            pass

        body = str.encode(self.driver.page_source)
        return HtmlResponse(
            self.driver.current_url, body=body, encoding="utf-8", request=request
        )

    def __del__(self):
        """Закрываем драйвер при завершении."""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Selenium WebDriver closed")
        except Exception:
            pass
