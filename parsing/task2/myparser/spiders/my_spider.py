import scrapy
import datetime
from myparser.items import MyparserItem


class MySpider(scrapy.Spider):
    """Основной паук для парсинга сайта books.toscrape.com."""
    name = "my_spider"
    allowed_domains = ["books.toscrape.com"]

    # Формируем список стартовых URL — например, первые 5 страниц каталога
    start_urls = [
        f"http://books.toscrape.com/catalogue/page-{i}.html" for i in range(1, 6)
    ]

    def parse(self, response):
        """Извлекаем ссылки на карточки товаров и обрабатываем пагинацию."""
        for href in response.css("h3 a::attr(href)").getall():
            yield response.follow(
                href,
                callback=self.parse_product,
                # meta={"use_selenium": True}  # 👈 middleware подхватит это
            )

        # Переход на следующую страницу (если есть)
        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        """Извлечение данных с карточки товара."""
        item = MyparserItem()
        item["url"] = response.url
        item["parse_time"] = datetime.datetime.utcnow()
        item["title"] = response.css("div.product_main h1::text").get()
        item["description"] = " ".join(
            response.css("#product_description ~ p::text").getall()
        ).strip()

        price_text = response.css("p.price_color::text").re_first(r"[\d\.,]+")
        item["price"] = float(price_text.replace(",", ".")) if price_text else None

        # Пример извлечения характеристик (на сайте их нет, оставляем шаблон)
        item["attributes"] = {
            "availability": response.css("p.availability::text").re_first(r"\w+")
        }

        item["comments"] = [
            {"text": c.get()}
            for c in response.css(".comment::text")
        ]

        item["links"] = response.css("a::attr(href)").getall()

        yield item
