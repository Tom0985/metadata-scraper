import argparse
import json
import logging
import os
import queue
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urldefrag, urlparse

# Ensure local imports work when executed directly
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from extractors.metadata_parser import parse_metadata
from extractors.utils import (
    fetch_html,
    find_links,
    match_any_glob,
    normalize_url,
)
from outputs.exporters import write_json

@dataclass
class CrawlConfig:
    start_urls: List[str]
    scrape_url_globs: List[str] = field(default_factory=list)
    pagination_url_globs: List[str] = field(default_factory=list)
    ignore_url_globs: List[str] = field(default_factory=list)
    max_requests_per_crawl: int = 100
    output_file: str = "data/output.json"
    delay_seconds: float = 0.0  # polite delay between requests

def load_input(input_path: str) -> CrawlConfig:
    with open(input_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Support both [{url: ""}] and [""] styles for startUrls
    start_urls = []
    for item in raw.get("startUrls", []):
        if isinstance(item, dict) and "url" in item:
            start_urls.append(item["url"])
        elif isinstance(item, str):
            start_urls.append(item)

    cfg = CrawlConfig(
        start_urls=start_urls,
        scrape_url_globs=raw.get("scrapeUrlGlobs", []),
        pagination_url_globs=raw.get("paginationUrlGlobs", []),
        ignore_url_globs=raw.get("ignoreUrlGlobs", []),
        max_requests_per_crawl=int(raw.get("maxRequestsPerCrawl", 100)),
        output_file=raw.get("outputFile", "data/output.json"),
        delay_seconds=float(raw.get("delaySeconds", 0.0)),
    )
    return cfg

def should_ignore(url: str, ignore_globs: List[str]) -> bool:
    return match_any_glob(url, ignore_globs)

def classify_url(
    url: str, scrape_globs: List[str], pagination_globs: List[str]
) -> str:
    """
    Returns one of: 'detail', 'pagination', 'unknown'
    """
    if match_any_glob(url, scrape_globs):
        return "detail"
    if match_any_glob(url, pagination_globs):
        return "pagination"
    return "unknown"

def crawl(cfg: CrawlConfig, logger: logging.Logger) -> List[Dict[str, Any]]:
    to_visit: deque[Tuple[str, Optional[str]]] = deque()
    visited: Set[str] = set()
    results: List[Dict[str, Any]] = []
    num_requests = 0

    # Seed queue
    for u in cfg.start_urls:
        norm = normalize_url(u)
        if norm:
            to_visit.append((norm, None))

    while to_visit and num_requests < cfg.max_requests_per_crawl:
        url, referrer = to_visit.popleft()
        if not url or url in visited:
            continue
        visited.add(url)

        if should_ignore(url, cfg.ignore_url_globs):
            logger.debug("Ignoring URL (ignore glob matched): %s", url)
            continue

        # Fetch HTML
        try:
            html, final_url, status = fetch_html(url)
            num_requests += 1
            if cfg.delay_seconds > 0:
                time.sleep(cfg.delay_seconds)
        except Exception as e:
            logger.warning("Failed to fetch %s (%s)", url, e)
            continue

        if status >= 400 or not html:
            logger.debug("Skipping URL due to status/no html: %s (%s)", url, status)
            continue

        # Re-normalize after redirects
        url = normalize_url(final_url) or url

        page_type = classify_url(url, cfg.scrape_url_globs, cfg.pagination_url_globs)
        logger.info("Fetched [%s] %s (referrer=%s)", page_type, url, referrer or "-")

        # If detail page, extract metadata
        if page_type == "detail" or (not cfg.scrape_url_globs and page_type != "pagination"):
            try:
                meta = parse_metadata(html, url)
                if meta:
                    results.append(meta)
            except Exception as e:
                logger.exception("Metadata extraction failed for %s: %s", url, e)

        # Discover new links from this page if it's pagination/unknown/start
        if page_type in ("pagination", "unknown"):
            try:
                links = find_links(html, base_url=url)
                for link in links:
                    norm = normalize_url(link)
                    if not norm or norm in visited:
                        continue
                    if should_ignore(norm, cfg.ignore_url_globs):
                        continue
                    # Only enqueue if likely relevant:
                    # - any pagination/detail match OR same origin as current url
                    classification = classify_url(norm, cfg.scrape_url_globs, cfg.pagination_url_globs)
                    if classification in ("detail", "pagination"):
                        to_visit.append((norm, url))
                    else:
                        # Fallback heuristic: same netloc as current URL
                        if urlparse(norm).netloc == urlparse(url).netloc:
                            to_visit.append((norm, url))
            except Exception as e:
                logger.debug("Link discovery failed for %s: %s", url, e)

    return results

def build_logger(verbosity: int) -> logging.Logger:
    log = logging.getLogger("metadata-scraper")
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    log.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    if not log.handlers:
        log.addHandler(handler)
    return log

def main():
    parser = argparse.ArgumentParser(description="Metadata Scraper Runner")
    parser.add_argument(
        "--input",
        "-i",
        default=os.environ.get("SCRAPER_INPUT", "data/inputs.sample.json"),
        help="Path to input JSON config (default: data/inputs.sample.json)",
    )
    parser.add_argument(
        "--out",
        "-o",
        default=os.environ.get("SCRAPER_OUTPUT"),
        help="Override output file (otherwise taken from input JSON)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Override max requests per crawl",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity (-v info, -vv debug)",
    )
    args = parser.parse_args()

    logger = build_logger(args.verbose)

    cfg = load_input(args.input)
    if args.out:
        cfg.output_file = args.out
    if args.max is not None:
        cfg.max_requests_per_crawl = args.max

    logger.info("Starting crawl with config: %s", cfg)

    results = crawl(cfg, logger)
    write_json(cfg.output_file, results)
    print(json.dumps({"count": len(results), "outputFile": cfg.output_file}, ensure_ascii=False))

if __name__ == "__main__":
    main()