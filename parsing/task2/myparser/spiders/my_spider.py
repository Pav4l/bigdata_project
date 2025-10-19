import scrapy
import datetime
from myparser.items import MyparserItem


class MySpider(scrapy.Spider):
    """Основной паук для парсинга сайта."""
    name = "my_spider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["http://books.toscrape.com/catalogue/page-{}.html"]

    def parse(self, response):
        """Извлекаем ссылки на карточки и страницы пагинации."""
        for href in response.css("a.product-link::attr(href)").getall():
            yield response.follow(
                href,
                callback=self.parse_product,
                meta={"use_selenium": True, "wait_for_css": "div.product"}
            )

        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        item = MyparserItem()
        item["url"] = response.url
        item["parse_time"] = datetime.datetime.utcnow()
        item["title"] = response.css("h1::text").get()
        item["description"] = " ".join(response.css("div.description *::text").getall()).strip()

        price_text = response.css("span.price::text").re_first(r"[\d\.,]+")
        item["price"] = float(price_text.replace(",", ".")) if price_text else None

        # Пример извлечения характеристик
        item["attributes"] = {
            "color": response.css(".color::text").get(),
            "brand": response.css(".brand::text").get()
        }

        # Пример извлечения комментариев
        item["comments"] = [
            {"text": c.get()}
            for c in response.css(".comment::text")
        ]

        # Все ссылки на странице
        item["links"] = response.css("a::attr(href)").getall()

        yield item
