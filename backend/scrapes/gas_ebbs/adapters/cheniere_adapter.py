"""
Cheniere source-family adapter.

Handles pipelines hosted on lngconnection.cheniere.com (3 pipelines).
Cheniere uses a React SPA. This adapter attempts to find and call the
underlying JSON API. Falls back to HTML parsing if available.

Pipelines: Corpus Christi (CCPL), Creole Trail (CTPL), Midship (MSPL)

EBB: https://lngconnection.cheniere.com/
"""

import json
import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id, DEFAULT_HEADERS
from backend.scrapes.gas_ebbs import outage_extractor


BASE_URL = "https://lngconnection.cheniere.com"


@register_adapter("cheniere")
class CheniereAdapter(EBBScraper):
    """Adapter for Cheniere LNG Connection EBB pages.

    Cheniere uses a React SPA with hash-based routing. The adapter
    attempts to find and call the underlying JSON API for notice data.
    Falls back to HTML table parsing.
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Cheniere LNG Connection detail page.

        Cheniere uses a React SPA, so detail pages may return JSON from
        an API endpoint or an HTML shell with embedded data. The adapter
        tries JSON parsing first, then looks for embedded React state,
        then falls back to generic HTML extraction.
        """
        # Try JSON parsing first (API response)
        try:
            data = json.loads(html)
            text_parts = []
            # Handle direct notice object
            notice_data = data
            if isinstance(data, dict):
                for key in ("data", "notice", "result", "posting"):
                    if key in data and isinstance(data[key], dict):
                        notice_data = data[key]
                        break
            if isinstance(notice_data, dict):
                for key in ("subject", "body", "content", "text",
                            "noticeText", "description", "detail"):
                    val = notice_data.get(key, "")
                    if val and isinstance(val, str):
                        text_parts.append(val)
            if text_parts:
                body_text = " ".join(text_parts)
                body_text = " ".join(body_text.split())
                extraction = outage_extractor.extract_outage(
                    subject=notice.get("subject", ""),
                    detail_text=body_text,
                )
                extraction["detail_text"] = body_text[:5000]
                return extraction
        except (json.JSONDecodeError, ValueError):
            pass

        # HTML parsing
        soup = BeautifulSoup(html, "html.parser")

        # Check for React/Next.js embedded state
        body_text = ""
        for script in soup.find_all("script"):
            script_text = script.string or ""
            if "notice" in script_text.lower() and len(script_text) > 100:
                # Try to extract JSON from the script
                for pattern in [r'__INITIAL_STATE__\s*=\s*({.*?});',
                                r'window\.__data\s*=\s*({.*?});',
                                r'"notice"\s*:\s*({.*?})\s*[,}]']:
                    match = re.search(pattern, script_text, re.DOTALL)
                    if match:
                        try:
                            obj = json.loads(match.group(1))
                            if isinstance(obj, dict):
                                for key in ("body", "content", "text",
                                            "description", "detail"):
                                    val = obj.get(key, "")
                                    if val and isinstance(val, str) and len(val) > 20:
                                        body_text = val
                                        break
                        except (json.JSONDecodeError, ValueError):
                            continue
                if body_text:
                    break

        if not body_text:
            # Remove scripts/styles for HTML extraction
            for tag in soup(["script", "style", "nav", "header", "footer"]):
                tag.decompose()

            # Look for React app root or content containers
            content = soup.find("div", id=re.compile(r"root|app|content|notice|detail", re.IGNORECASE))
            if not content:
                content = soup.find("div", class_=re.compile(r"notice|content|detail", re.IGNORECASE))
            if not content:
                content = soup.find("main") or soup.find("article")

            if content:
                body_text = content.get_text(separator=" ", strip=True)
            else:
                body_text = soup.get_text(separator=" ", strip=True)

        body_text = " ".join(body_text.split())

        extraction = outage_extractor.extract_outage(
            subject=notice.get("subject", ""),
            detail_text=body_text,
        )
        extraction["detail_text"] = body_text[:5000]
        return extraction

    def _get_listing_sources(self) -> list[dict]:
        """Return the API/page URL for the pipeline."""
        pipe_code = self.config["pipe_code"]
        base = self.config.get("base_url", BASE_URL)

        # Try common API patterns for React SPAs
        return [
            {
                "url": f"{base}/api/{pipe_code}/notices",
                "pipe_code": pipe_code,
                "label": f"{self.pipeline_name}",
            }
        ]

    def _pull(self, url: str = "") -> str:
        """Fetch from API endpoint, with JSON and HTML fallbacks."""
        import requests

        target = url or self.listing_url
        pipe_code = self.config["pipe_code"]
        base = self.config.get("base_url", BASE_URL)

        # Try JSON API patterns
        api_urls = [
            target,
            f"{base}/api/v1/{pipe_code}/notices",
            f"{base}/api/notices?pipeline={pipe_code}",
            f"{base}/#{pipe_code}",
        ]

        api_headers = {
            **DEFAULT_HEADERS,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }

        for api_url in api_urls:
            try:
                response = requests.get(api_url, headers=api_headers, timeout=30)
                if response.status_code == 200:
                    content_type = response.headers.get("Content-Type", "")
                    if "json" in content_type:
                        return response.text
                    try:
                        json.loads(response.text)
                        return response.text
                    except (json.JSONDecodeError, ValueError):
                        # Return HTML content from last successful request
                        return response.text
            except requests.RequestException:
                continue

        # Final fallback: fetch the SPA page
        response = requests.get(
            f"{base}/#{pipe_code}",
            headers=DEFAULT_HEADERS,
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        pipe_code = kwargs.get("pipe_code", self.config.get("pipe_code", ""))
        base = self.config.get("base_url", BASE_URL)

        # Try JSON parsing
        try:
            data = json.loads(html)
            notices = self._parse_json_data(data, pipe_code, base)
            if notices:
                return notices
        except (json.JSONDecodeError, ValueError):
            pass

        # Try to find JSON embedded in the HTML
        json_match = re.search(r'(?:data|notices|postings)\s*[=:]\s*(\[.*?\])', html, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                notices = self._parse_json_data(data, pipe_code, base)
                if notices:
                    return notices
            except (json.JSONDecodeError, ValueError):
                pass

        # Fallback: HTML table parsing
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return []

        data_table = max(tables, key=lambda t: len(t.find_all("tr")))
        rows = data_table.find_all("tr")
        notices = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            notice_id = None
            for i, cell in enumerate(cells):
                candidate = clean_text(cell)
                if candidate and candidate.strip().isdigit():
                    notice_id = candidate
                    break

            if not notice_id:
                continue

            notice = {
                "notice_type": clean_text(cells[0]),
                "notice_subtype": "",
                "posted_datetime": clean_text(cells[1]) if len(cells) > 1 else "",
                "effective_datetime": clean_text(cells[2]) if len(cells) > 2 else "",
                "end_datetime": clean_text(cells[3]) if len(cells) > 3 else "",
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": clean_text(cells[4]) if len(cells) > 4 else "",
                "response_datetime": "",
                "detail_url": f"{base}/#{pipe_code}/notice/{notice_id}",
            }
            notices.append(notice)

        return notices

    def _parse_json_data(self, data, pipe_code: str, base: str) -> list[dict]:
        """Parse JSON notice data from API response."""
        if isinstance(data, dict):
            for key in ("data", "notices", "results", "items", "postings"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                if not isinstance(data, list):
                    return []

        if not isinstance(data, list):
            return []

        notices = []
        for item in data:
            if not isinstance(item, dict):
                continue

            notice_id = str(
                item.get("noticeId") or item.get("NoticeId")
                or item.get("notice_id") or item.get("id") or ""
            )
            if not notice_id or not extract_numeric_id(notice_id):
                continue

            notice = {
                "notice_type": str(item.get("noticeType", item.get("type", ""))),
                "notice_subtype": "",
                "posted_datetime": str(item.get("postedDate", item.get("postDateTime", ""))),
                "effective_datetime": str(item.get("effectiveDate", item.get("effectiveDateTime", ""))),
                "end_datetime": str(item.get("endDate", item.get("endDateTime", ""))),
                "notice_identifier": notice_id,
                "notice_status": str(item.get("status", "")),
                "subject": str(item.get("subject", item.get("Subject", ""))),
                "response_datetime": str(item.get("responseDate", item.get("responseDateTime", ""))),
                "detail_url": f"{base}/#{pipe_code}/notice/{notice_id}",
            }
            notices.append(notice)

        return notices
