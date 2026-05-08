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
    "spidermon.contrib.scrapy.extensions.Spidermon": 500,
}

# Spidermon (Phase 9). RunIdExtension at 100 runs first so spider.run_id
# exists by the time any monitor reads stats. We deliberately do NOT
# enable SPIDERMON_VALIDATION_MODELS — we already validate items via
# ValidationPipeline (Phase 2.3); Spidermon's pydantic-v1 path would
# duplicate the work and may not align with our v2 schema.
SPIDERMON_ENABLED = True
# IMPORTANT: register the close suite under ENGINE_STOPPED, not
# SPIDER_CLOSE. spider_closed signal fires for both Spidermon
# (via priority-500 extension) and our pipelines (priority 200/400
# /500/600 via pipeline manager registration). Pipelines register
# AFTER Spidermon, so on spider_closed the monitor would race
# them and read pre-flush postgres/gsheets/gdrive stats.
# engine_stopped fires once every spider_closed handler completes,
# so all pipeline counters are final by the time monitors run.
SPIDERMON_ENGINE_STOP_MONITORS = (
    "broker_scout.monitors.monitors.SpiderCloseMonitorSuite",
)
SPIDERMON_PERIODIC_MONITORS = {
    "broker_scout.monitors.monitors.PeriodicMonitorSuite": 60,
}
# Thresholds for Phase 9.0 built-ins. Phases 9.1–9.3 add many more.
SPIDERMON_MAX_ERRORS = 500
# Mirrors pipelines/postgres.py::SUCCESSFUL_REASONS so monitor verdict
# and scrape_runs.status agree on what counts as a successful run.
SPIDERMON_EXPECTED_FINISH_REASONS = (
    "finished",
    "closespider_itemcount",
    "closespider_pagecount",
    "closespider_timeout",
)
# Phase 9.1 — validation suite thresholds.
# Overall validation failure rate; >5% suggests a systemic issue.
VALIDATION_FAILURE_RATE_THRESHOLD = 0.05
# Per-field failure rate; >10% on a single field signals PF schema drift.
VALIDATION_FIELD_FAILURE_RATE_THRESHOLD = 0.10

# Phase 9.2 — periodic circuit-breaker thresholds.
# Mid-run 429 count above this triggers a circuit-breaker close.
# Decoupled from the close-suite UnwantedHTTPCodesMonitor (9.3.3)
# so we can keep the periodic threshold tight while letting the
# close-suite check use a different value if needed.
PERIODIC_429_THRESHOLD = 50

# Phase 9.3.3 — close-suite HTTP/network thresholds.
# Map of HTTP code → max allowed responses for a healthy run. Note:
# 404 is NOT here — empirically PF returns 404 for many DLD broker
# names that aren't on PF, and that's expected (becomes a not_found
# stub via match_status). Alarming on 404 would create constant
# false positives.
SPIDERMON_UNWANTED_HTTP_CODES = {403: 20, 429: 100, 503: 5}
RETRY_RATE_THRESHOLD = 0.15

# Phase 9.3.7 — match-coverage thresholds.
# (exact_brn + name_unique) / total ≥ 60% — high-confidence rate.
MATCH_HIGH_CONFIDENCE_RATE_THRESHOLD = 0.60
NOT_FOUND_RATE_THRESHOLD = 0.50
AMBIGUOUS_RATE_THRESHOLD = 0.05
# Any PF↔DLD BRN disagreement is worth surfacing.
BRN_DRIFT_THRESHOLD = 0

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
