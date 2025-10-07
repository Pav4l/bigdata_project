from bs4 import BeautifulSoup


def parse_page(html: str) -> list[dict]:
    """Парсит HTML и возвращает список книг."""
    soup = BeautifulSoup(html, "html.parser")
    books = []

    for book in soup.select(".product_pod"):
        title = book.h3.a["title"]
        price = book.select_one(".price_color").text.strip()
        availability = book.select_one(".availability").text.strip()

        books.append({
            "title": title,
            "price": price,
            "availability": availability
        })
    return books
