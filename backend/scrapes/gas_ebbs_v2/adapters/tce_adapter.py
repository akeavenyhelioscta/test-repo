"""
TC Energy (TCE Connects) source-family adapter.

Handles all pipelines hosted on www.tceconnects.com/infopost (11 pipelines).
TC Energy serves notice data via jqGrid JSON endpoints:
    webmethods/SSRS_ListCriticalNotices.aspx?assetid={assetid}&page=1&rows=500
    webmethods/SSRS_ListNonCriticalNotices.aspx?assetid={assetid}&page=1&rows=500

Each row is: {"id": "25986110", "cell": ["notice_id", "subject", "posted_date"]}

Supported pipelines: ANR, ANRS, BISON, BLGS, TCO, CGUL, CROSS, HARDY,
                     MILL, NBPL, PNGTS

EBB: https://www.tceconnects.com/infopost/
"""

import json
import re

import requests
from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs_v2.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs_v2.ebb_utils import DEFAULT_HEADERS
from backend.scrapes.gas_ebbs_v2 import outage_extractor


BASE_URL = "https://www.tceconnects.com/infopost"

NOTICE_TYPE_LABELS = {
    "critical": "Critical",
    "non-critical": "Non-Critical",
}

# jqGrid endpoint names by notice type
GRID_ENDPOINTS = {
    "critical": "webmethods/SSRS_ListCriticalNotices.aspx",
    "non-critical": "webmethods/SSRS_ListNonCriticalNotices.aspx",
}

ROWS_PER_PAGE = 500


@register_adapter("tce")
class TCEAdapter(EBBScraper):
    """Adapter for TC Energy (TCE Connects) EBB pages.

    TC Energy's infopost site is a jqGrid SPA. The notice data is served
    via ASP.NET endpoints that return jqGrid-compatible JSON when called
    with page/rows query parameters.

    JSON response format:
        {"total": "6", "page": "1", "records": "51", "rows": [
            {"id": "25986110", "cell": ["notice_id", "subject", "posted_date"]},
            ...
        ]}
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse TC Energy (TCE Connects) detail page.

        TCE detail pages (MobileInfoPost.aspx) may return JSON data
        or an HTML page with the notice body. The adapter tries JSON
        parsing first, then falls back to HTML extraction.
        """
        # Try JSON parsing first — TCE detail endpoints may return JSON
        try:
            data = json.loads(html)
            # Extract text fields from JSON response
            text_parts = []
            if isinstance(data, dict):
                for key in ("subject", "body", "content", "text",
                            "noticeText", "description", "detail"):
                    val = data.get(key, "")
                    if val and isinstance(val, str):
                        text_parts.append(val)
                # Also check nested structures
                for key in ("notice", "data", "result"):
                    nested = data.get(key, {})
                    if isinstance(nested, dict):
                        for nk in ("subject", "body", "content", "text",
                                    "noticeText", "description"):
                            val = nested.get(nk, "")
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

        # HTML parsing fallback
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # TCE pages use ASP.NET panels — look for content containers
        content = soup.find("div", id=re.compile(r"content|notice|detail|pnl", re.IGNORECASE))
        if not content:
            content = soup.find("div", class_=re.compile(r"content|notice|detail", re.IGNORECASE))

        if content:
            body_text = content.get_text(separator=" ", strip=True)
        else:
            # Fallback: largest text block
            divs = soup.find_all("div")
            longest = ""
            for div in divs:
                text = div.get_text(separator=" ", strip=True)
                if len(text) > len(longest):
                    longest = text
            body_text = longest if len(longest) > 100 else soup.get_text(separator=" ", strip=True)

        body_text = " ".join(body_text.split())

        extraction = outage_extractor.extract_outage(
            subject=notice.get("subject", ""),
            detail_text=body_text,
        )
        extraction["detail_text"] = body_text[:5000]
        return extraction

    def _get_listing_sources(self) -> list[dict]:
        """Return jqGrid JSON URLs for critical and non-critical notices."""
        assetid = self.config.get("assetid", "")
        if not assetid:
            return []

        base = self.config.get("base_url", BASE_URL)
        notice_types = self.config.get("notice_types", ["critical", "non-critical"])

        sources = []
        for nt in notice_types:
            endpoint = GRID_ENDPOINTS.get(nt)
            if not endpoint:
                continue
            url = f"{base}/{endpoint}?assetid={assetid}&page=1&rows={ROWS_PER_PAGE}"
            sources.append({
                "url": url,
                "notice_type_code": nt,
                "label": f"{self.pipeline_name} ({NOTICE_TYPE_LABELS.get(nt, nt)})",
            })
        return sources

    def _pull(self, url: str = "") -> str:
        """Fetch jqGrid JSON data."""
        target = url or self.listing_url
        response = requests.get(target, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        return response.text

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "critical")
        assetid = self.config.get("assetid", "")
        base = self.config.get("base_url", BASE_URL)

        try:
            data = json.loads(html)
        except (json.JSONDecodeError, ValueError):
            return []

        rows = data.get("rows", [])
        if not rows:
            return []

        notices = []
        for row in rows:
            cell = row.get("cell", [])
            if len(cell) < 3:
                continue

            notice_id = str(cell[0]).strip()
            subject = str(cell[1]).strip()
            posted_date = str(cell[2]).strip()

            if not notice_id:
                continue

            detail_url = (
                f"{base}/MobileInfoPost.aspx"
                f"?assetid={assetid}&noticeId={notice_id}"
            )

            notices.append({
                "notice_type": NOTICE_TYPE_LABELS.get(notice_type_code, notice_type_code),
                "notice_subtype": notice_type_code,
                "posted_datetime": posted_date,
                "effective_datetime": posted_date,
                "end_datetime": "",
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": subject,
                "response_datetime": "",
                "detail_url": detail_url,
            })

        # Handle pagination: fetch additional pages if needed
        total_pages = int(data.get("total", "1"))
        if total_pages > 1:
            url_base = kwargs.get("url", self.listing_url).split("&page=")[0]
            for page in range(2, total_pages + 1):
                page_url = f"{url_base}&page={page}&rows={ROWS_PER_PAGE}"
                try:
                    page_text = self._pull(page_url)
                    page_data = json.loads(page_text)
                    for row in page_data.get("rows", []):
                        cell = row.get("cell", [])
                        if len(cell) < 3:
                            continue
                        notice_id = str(cell[0]).strip()
                        subject = str(cell[1]).strip()
                        posted_date = str(cell[2]).strip()
                        if not notice_id:
                            continue
                        notices.append({
                            "notice_type": NOTICE_TYPE_LABELS.get(notice_type_code, notice_type_code),
                            "notice_subtype": notice_type_code,
                            "posted_datetime": posted_date,
                            "effective_datetime": posted_date,
                            "end_datetime": "",
                            "notice_identifier": notice_id,
                            "notice_status": "",
                            "subject": subject,
                            "response_datetime": "",
                            "detail_url": (
                                f"{base}/MobileInfoPost.aspx"
                                f"?assetid={assetid}&noticeId={notice_id}"
                            ),
                        })
                except Exception:
                    break

        return notices
