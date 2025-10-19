from scrapy import Item, Field


class MyparserItem(Item):
    url = Field()
    parse_time = Field()
    title = Field()
    description = Field()
    price = Field()
    attributes = Field()
    comments = Field()
    links = Field()
