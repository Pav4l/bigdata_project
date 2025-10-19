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
    """
    Использует Selenium для обработки динамических страниц (AJAX, скролл и т.п.)
    """
    def __init__(self, timeout=30):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, timeout)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(timeout=crawler.settings.getint("SELENIUM_TIMEOUT", 30))

    def process_request(self, request, spider):
        """Если в meta передан use_selenium=True — используем Selenium."""
        if not request.meta.get("use_selenium"):
            return None

        self.driver.get(request.url)

        # ожидание появления элемента
        wait_css = request.meta.get("wait_for_css")
        if wait_css:
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_css)))
            except Exception:
                logger.warning(f"Timeout waiting for {wait_css}")

        # прокрутка страницы
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        body = str.encode(self.driver.page_source)
        return HtmlResponse(self.driver.current_url, body=body, encoding="utf-8", request=request)

    def __del__(self):
        try:
            self.driver.quit()
        except Exception:
            pass
