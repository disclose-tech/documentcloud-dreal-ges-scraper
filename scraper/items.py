"""Models for the scraped items."""

from scrapy.item import Item, Field


class DocumentItem(Item):
    """A document that will be uploaded to DocumentCloud."""

    title = Field()
    project = Field()
    project_id = Field()

    source = Field()
    access = Field()

    authority = Field()

    category = Field()
    category_local = Field()

    source_scraper = Field()
    source_file_url = Field()
    source_filename = Field()
    source_page_url = Field()

    publication_date = Field()
    publication_time = Field()
    publication_datetime = Field()
    publication_datetime_dcformat = Field()

    publication_lastmodified = Field()

    full_info = Field()

    year = Field()

    headers = Field()

    department_from_scraper = Field()
    departments = Field()
    departments_sources = Field()
