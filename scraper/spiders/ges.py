import re
from datetime import datetime, timedelta

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem


class GESSpider(scrapy.Spider):

    name = "DREAL GES Scraper"

    # allowed_domains = ["grand-est.developpement-durable.gouv.fr"]

    start_urls = [
        "https://www.grand-est.developpement-durable.gouv.fr/avis-et-decisions-de-l-ae-r6433.html?lang=fr"
    ]

    upload_limit_attained = False

    start_time = datetime.now()

    def check_time_limit(self):
        """Closes the spider automatically if it reaches a duration of 5h45min"""
        """as GitHub's actions have a 6 hours limit."""

        if self.time_limit != 0:

            limit = self.time_limit * 60
            now = datetime.now()

            if timedelta.total_seconds(now - self.start_time) > limit:
                raise CloseSpider(
                    f"Closed due to time limit ({self.time_limit} minutes)"
                )

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def parse(self, response):
        """Parse the starting page"""

        rubriques_avec_sous_rubriques = response.css(
            "#contenu .liste-rubriques .rubrique_avec_sous-rubriques"
        )

        for rubrique in rubriques_avec_sous_rubriques:

            titre_rubrique = rubrique.css(".fr-tile__title::text").get().strip()

            if str(self.target_year) in titre_rubrique:

                self.logger.info(f"Scraping {titre_rubrique}")

                dept_links = rubrique.css("a.lien-sous-rubrique")

                for dl in dept_links:

                    dept = dl.css("::text").get()
                    link_url = dl.attrib["href"]

                    yield response.follow(
                        link_url,
                        callback=self.parse_projects_list,
                        cb_kwargs=dict(dept=dept, page=1),
                    )

    def parse_projects_list(self, response, dept, page):
        """Parse projects list for a year & department."""

        self.check_time_limit()
        self.check_upload_limit()

        self.logger.info(f"Scraping {dept}, page {page}")

        # yield project pages

        projects_links = response.css("#contenu .fr-card__link")

        for link in projects_links:
            link_text = link.css("::text").get()
            link_url = link.attrib["href"]

            # print(f"Seen: {link_text} at {link_url}")

            yield response.follow(
                link_url,
                callback=self.parse_project_page,
                cb_kwargs=dict(dept=dept),
            )

        # next page

        next_page_link = response.css(
            "#contenu .fr-pagination__list .fr-pagination__link--next[href]"
        )

        if next_page_link:

            next_page_url = next_page_link.attrib["href"]

            yield response.follow(
                next_page_url,
                callback=self.parse_projects_list,
                cb_kwargs=dict(dept=dept, page=page + 1),
            )

    def parse_project_page(self, response, dept):
        """Parse the page of a project."""

        self.check_time_limit()
        self.check_upload_limit()

        file_links = response.css(
            "#contenu .contenu-article div.fr-download--card a.fr-download__link"
        )

        if file_links:

            project_name_strings = (
                response.css("#contenu .texte-article > p")[0].css("*::text").getall()
            )

            project_name = "".join(project_name_strings)

            # # Process files

            for link in file_links:
                link_text = link.css("::text").get().strip()
                link_url = link.attrib["href"]

                full_link_url = response.urljoin(link_url)

                doc_prefix = (
                    link.xpath("../../preceding-sibling::p[1]").css("::text").get()
                )

                doc_prefix = doc_prefix.rstrip(":\xa0 ;<")

                if full_link_url not in self.event_data:

                    doc_item = DocumentItem(
                        title=f"{doc_prefix} ({link_text})",
                        source_page_url=response.request.url,
                        project=project_name,
                        year=self.target_year,
                        authority="Préfecture de région Grand Est",
                        category_local="Décisions cas par cas projets",
                        source_scraper=f"DREAL GES Scraper {self.target_year}",
                        # full_info=info,
                        source="www.grand-est.developpement-durable.gouv.fr",
                        access=self.access_level,
                        department_from_scraper=dept[-3:-1],
                    )

                    yield response.follow(
                        link_url,
                        method="HEAD",
                        callback=self.parse_document_headers,
                        cb_kwargs=dict(doc_item=doc_item),
                    )

    def parse_document_headers(self, response, doc_item):

        self.check_time_limit()
        self.check_upload_limit()

        doc_item["source_file_url"] = response.request.url

        doc_item["publication_lastmodified"] = response.headers.get(
            "Last-Modified"
        ).decode("utf-8")

        yield doc_item
