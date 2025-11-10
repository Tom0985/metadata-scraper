from typing import Dict, Optional
from bs4 import BeautifulSoup
from readability import Document
import re

def _text_or_none(node) -> Optional[str]:
    if not node:
        return None
    txt = node.get_text(separator=" ", strip=True)
    return txt or None

def _first_meta(soup: BeautifulSoup, names) -> Optional[str]:
    if isinstance(names, str):
        names = [names]
    # Search by name= or property=
    for n in names:
        meta = soup.find("meta", attrs={"name": n})
        if meta and meta.get("content"):
            return meta["content"].strip()
        meta = soup.find("meta", attrs={"property": n})
        if meta and meta.get("content"):
            return meta["content"].strip()
    return None

def _clean_whitespace(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    # Collapse excessive whitespace
    return re.sub(r"\s+", " ", text).strip()

def parse_metadata(html: str, url: str) -> Dict[str, Optional[str]]:
    """
    Extracts core metadata from an HTML document.
    Includes:
      - url
      - title (prefers og:title, dc:title, then <title> or first h1)
      - description (prefers meta description/og:description)
      - heading (first h1 or h2)
      - article (readability-extracted text fallback to main/article/body)
    """
    soup = BeautifulSoup(html, "lxml")

    # Title resolution priority
    title = (
        _first_meta(soup, ["og:title", "twitter:title", "dc.title"])
        or (soup.title.string.strip() if soup.title and soup.title.string else None)
    )
    if not title:
        first_h1 = soup.find("h1")
        if first_h1:
            title = _text_or_none(first_h1)

    # Description
    description = _first_meta(soup, ["description", "og:description", "twitter:description"])

    # Heading
    heading = None
    h = soup.find("h1") or soup.find("h2")
    if h:
        heading = _text_or_none(h)

    # Article: use Readability to get the main content
    article = None
    try:
        doc = Document(html)
        content_html = doc.summary(html_partial=True)
        content_soup = BeautifulSoup(content_html, "lxml")
        # Extract visible text from the summary
        article = _text_or_none(content_soup)
    except Exception:
        article = None

    # Fallback for article if readability fails
    if not article:
        main = soup.find("article") or soup.find("main")
        if main:
            article = _text_or_none(main)
        else:
            # take largest text block as last resort
            paras = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
            paras = [p for p in paras if p]
            if paras:
                # choose top ~10 paragraphs or join all if short
                joined = " ".join(paras[:10])
                article = joined if joined else None

    data = {
        "url": url,
        "title": _clean_whitespace(title),
        "description": _clean_whitespace(description),
        "heading": _clean_whitespace(heading),
        "article": _clean_whitespace(article),
    }
    return data