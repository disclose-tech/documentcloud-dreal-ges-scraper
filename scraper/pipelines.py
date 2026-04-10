# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json
import hashlib
import sys

from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from documentcloud.constants import SUPPORTED_EXTENSIONS

from .log import SilentDropItem
from .departments import department_from_authority, departments_from_project_name

from .corrections import project_name_corrections


class SpiderPipeline:
    """Base class for pipelines that need access to the spider instance.

    Provides from_crawler() to store spider as self.spider.
    Inherit from this class instead of defining from_crawler() in each pipeline.
    """

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.spider = crawler.spider
        return pipeline


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item):
        """Parses date from the extracted string"""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
        )

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")
        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        item["publication_datetime_dcformat"] = (
            publication_dt.isoformat(timespec="microseconds") + "Z"
        )

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item):

        if "cas par cas" in item["category_local"].lower():
            item["category"] = "Cas par cas"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class BeautifyPipeline:
    def process_item(self, item):
        """Beautify & harmonize project names & document titles."""

        # Project

        item["project"] = item["project"].strip()
        item["project"] = (
            item["project"].replace(" ", " ").replace("’", "'").replace("\n", ", ")
        )
        item["project"] = item["project"].rstrip(".,:}")

        if item["source_page_url"] in project_name_corrections:
            item["project"] = project_name_corrections[item["source_page_url"]]

        # Title

        item["title"] = item["title"].strip()
        item["title"] = item["title"][0].upper() + item["title"][1:]

        return item


class UnsupportedFiletypePipeline:

    def process_item(self, item):

        filename, file_extension = os.path.splitext(item["source_filename"])
        file_extension = file_extension.lower()

        if file_extension not in SUPPORTED_EXTENSIONS:
            # Drop the item
            raise DropItem("Unsupported filetype")
        else:
            return item


class UploadLimitPipeline(SpiderPipeline):
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self):
        self.number_of_docs = 0

    def process_item(self, item):
        self.number_of_docs += 1

        if (
            self.spider.upload_limit == 0
            or self.number_of_docs <= self.spider.upload_limit
        ):
            return item
        else:
            self.spider.upload_limit_attained = True
            raise SilentDropItem("Upload limit exceeded.")


class TagDepartmentsPipeline:

    def process_item(self, item):

        item["departments"] = [item["department_from_scraper"]]
        item["departments_sources"] = ["scraper"]

        authority_department = department_from_authority(item["authority"])

        if (
            authority_department
            and authority_department != item["department_from_scraper"]
        ):
            item["departments_sources"].append("authority")
            item["departments"].append(authority_department)

        else:

            project_departments = departments_from_project_name(item["project"])

            if project_departments and project_departments != item["departments"]:
                item["departments_sources"].append("regex")
                item["departments"].extend(project_departments)

        if item["departments"]:
            item["departments"] = sorted(list(set(item["departments"])))

        return item


class ProjectIDPipeline:

    def process_item(self, item):

        project_name = item["project"]
        source_page_url = item["source_page_url"]
        string_to_hash = source_page_url + " " + project_name

        hash_object = hashlib.sha256(string_to_hash.encode())
        hex_dig = hash_object.hexdigest()

        item["project_id"] = hex_dig

        return item


class UploadPipeline(SpiderPipeline):
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)

        if not self.spider.dry_run:
            try:
                self.spider.logger.info("Loading event data from DocumentCloud...")
                self.spider.event_data = self.spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            # Load from json if present
            try:
                self.spider.logger.info("Loading event data from local JSON file...")
                with open("event_data.json", "r") as file:
                    data = json.load(file)

                    self.spider.event_data = data
            except:
                self.spider.event_data = None

        if self.spider.event_data:
            self.spider.logger.info(
                f"Loaded event data ({len(self.spider.event_data)} documents)"
            )
        else:
            self.spider.logger.info("No event data was loaded.")
            self.spider.event_data = {}

    def process_item(self, item):

        data = {
            "authority": item["authority"],
            "category": item["category"],
            "category_local": item["category_local"],
            "source_scraper": "DREAL GES Scraper",
            "source_scraper_year": str(item["year"]),
            "source_file_url": item["source_file_url"],
            "event_data_key": item["source_file_url"],
            "source_page_url": item["source_page_url"],
            "source_filename": item["source_filename"],
            "publication_date": item["publication_date"],
            "publication_time": item["publication_time"],
            "publication_datetime": item["publication_datetime"],
            "project_id": item["project_id"],
        }

        adapter = ItemAdapter(item)
        if adapter.get("departments") and adapter.get("departments_sources"):
            data["departments"] = item["departments"]
            data["departments_sources"] = item["departments_sources"]

        try:
            if not self.spider.dry_run:
                self.spider.client.documents.upload(
                    item["source_file_url"],
                    project=self.spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    publish_at=item["publication_datetime_dcformat"],
                    source=item["source"],
                    language="fra",
                    access=item["access"],
                    data=data,
                )
                self.spider.logger.info(
                    f"Uploaded {item['source_file_url']} to DocumentCloud"
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)
        else:  # No upload error, add to event_data

            last_modified = datetime.datetime.strptime(
                item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
            ).isoformat()
            now = datetime.datetime.now().isoformat(timespec="seconds")

            self.spider.event_data[item["source_file_url"]] = {
                "last_modified": last_modified,
                "last_seen": now,
                "target_year": self.spider.target_year,
            }

            # # Save event data after each upload
            if (
                self.spider.run_id and not self.spider.dry_run
            ):  # only from the web interface
                self.spider.store_event_data(self.spider.event_data)

        return item

    def close_spider(self):
        """Store event data when the spider closes."""

        if not self.spider.dry_run and self.spider.run_id:
            self.spider.store_event_data(self.spider.event_data)
            self.spider.logger.info(
                f"Uploaded event data ({len(self.spider.event_data)} documents)"
            )

            if self.spider.upload_event_data:

                # Upload the event_data to the DocumentCloud interface
                now = datetime.datetime.now()
                timestamp = now.strftime("%Y%m%d_%H%M")
                filename = f"event_data_DREAL_GES_{timestamp}.json"

                with open(filename, "w+") as event_data_file:
                    json.dump(self.spider.event_data, event_data_file)
                    self.spider.upload_file(event_data_file)
                self.spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not self.spider.run_id:
            with open("event_data.json", "w") as file:
                json.dump(self.spider.event_data, file)
                self.spider.logger.info(
                    f"Saved file event_data.json ({len(self.spider.event_data)} documents)"
                )


class MailPipeline(SpiderPipeline):
    """Send scraping run report."""

    def open_spider(self):
        self.scraped_items = []

    def process_item(self, item):

        self.scraped_items.append(item)

        return item

    def close_spider(self):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            authority: {item["authority"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            return item_string

        subject = f"DREAL GES Scraper {str(self.spider.target_year)} (New: {len(self.scraped_items)}) [{self.spider.run_name}]"

        start_content = f"DREAL GES Scraper Addon Run {self.spider.run_id}"

        scraped_items_content = (
            f"SCRAPED ITEMS ({len(self.scraped_items)})\n\n"
            + "\n\n".join([print_item(item) for item in self.scraped_items])
        )

        content = "\n\n".join([start_content, scraped_items_content])

        if not self.spider.dry_run:
            self.spider.send_mail(subject, content)
