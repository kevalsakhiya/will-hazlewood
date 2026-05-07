"""Scrapy settings for the broker_scout project."""

import os

from dotenv import load_dotenv

from broker_scout.utils.logging_setup import configure_logging

load_dotenv()

BOT_NAME = "broker_scout"

SPIDER_MODULES = ["broker_scout.spiders"]
NEWSPIDER_MODULE = "broker_scout.spiders"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_ENABLED = False  # we install our own JSON handler in configure_logging()
configure_logging(LOG_LEVEL)

EXTENSIONS = {
    "broker_scout.extensions.RunIdExtension": 100,
}

# Filled phase by phase per roadmap.md priority table:
#   100 normalization · 200 validation · 300 dedupe ·
#   400 postgres     · 500 gsheets    · 600 gdrive_csv
ITEM_PIPELINES: dict[str, int] = {
    "broker_scout.pipelines.validation.ValidationPipeline": 200,
    "broker_scout.pipelines.postgres.PostgresPipeline": 400,
}

CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

FEED_EXPORT_ENCODING = "utf-8"
