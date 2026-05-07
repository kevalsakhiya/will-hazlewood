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
    "broker_scout.pipelines.gsheets.GSheetsBatchPipeline": 500,
    "broker_scout.pipelines.gdrive_csv.GDriveCsvPipeline": 600,
}

CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

FEED_EXPORT_ENCODING = "utf-8"

# Phase 6 — DLD-seeded spider tunables
# Optional smoke-test cap: only seed N DLD brokers per run. Unset/0 = full registry.
DLD_LIMIT = int(os.getenv("DLD_LIMIT", "0"))
# Comma-separated BRNs. When set, only these DLD brokers seed the
# spider — useful for replay or focused dev testing.
DLD_BRN_FILTER = os.getenv("DLD_BRN_FILTER", "")
# rapidfuzz token-set ratio cutoff for name-fuzzy matches (0..100).
MATCH_FUZZY_THRESHOLD = int(os.getenv("MATCH_FUZZY_THRESHOLD", "90"))
