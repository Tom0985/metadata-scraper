import logging
import re
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup
import fnmatch

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    # Drop fragments and trim
    url, _frag = urldefrag(url.strip())
    parsed = urlparse(url)
    if not parsed.scheme:
        # reject invalids
        return None
    # Normalize trailing slash for consistency
    if parsed.scheme in ("http", "https") and not parsed.path:
        url = f"{parsed.scheme}://{parsed.netloc}/"
    return url

def fetch_html(url: str, timeout: int = 20) -> Tuple[str, str, int]:
    """Returns (html, final_url, status_code) or raises."""
    with requests.Session() as s:
        s.headers.update(DEFAULT_HEADERS)
        resp = s.get(url, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp.text, str(resp.url), resp.status_code

def find_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        # Skip mailto/tel/javascript
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        abs_url = urljoin(base_url, href)
        n = normalize_url(abs_url)
        if n:
            links.append(n)
    return list(dict.fromkeys(links))  # de-duplicate preserving order

def match_any_glob(target: str, globs: Iterable[str]) -> bool:
    """Return True if target matches any of the provided glob patterns.
    Supports '*' and '?' wildcards, and '**' to span path separators."""
    for pattern in globs or []:
        # Convert "**" to "*" variant since URLs treat '/' as character
        # fnmatch with '**' behaves acceptably as literal '**', so expand:
        pat = pattern.replace("**", "*")
        if fnmatch.fnmatch(target, pat):
            return True
    return False