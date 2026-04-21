"""
DT Midstream (Trellis Energy) source-family adapter.

Handles pipelines hosted on dtmidstream.trellisenergy.com (3 pipelines).
Trellis portal serves notices as links with data-noticesdata attributes
containing stringified JSON. Also checks for a public API endpoint.

Pipelines: Guardian (GPL), Midwestern (MGT), Viking (VGT)

EBB: https://dtmidstream.trellisenergy.com/
"""

import json
import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id, DEFAULT_HEADERS
from backend.scrapes.gas_ebbs import outage_extractor


BASE_URL = "https://dtmidstream.trellisenergy.com"


@register_adapter("dtmidstream")
class DTMidstreamAdapter(EBBScraper):
    """Adapter for DT Midstream (Trellis Energy) EBB pages.

    Trellis displays notices as links with class ``notice-event-link``
    and ``data-noticesdata`` attributes containing stringified JSON.

    Also attempts to call a public API endpoint for structured data.
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse DT Midstream (Trellis Energy) detail page.

        Trellis detail pages may contain notice data as JSON in
        data-noticesdata attributes, or render content in div-based
        layouts. The adapter checks for JSON data first, then extracts
        from HTML.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove scripts, styles, and navigation
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # Try to find notice data in data attributes
        notice_el = soup.find(attrs={"data-noticesdata": True})
        if notice_el:
            try:
                data = json.loads(notice_el["data-noticesdata"])
                if isinstance(data, dict):
                    text_parts = []
                    for key in ("subject", "body", "content", "text",
                                "description", "typeDesc"):
                        val = data.get(key, "")
                        if val and isinstance(val, str):
                            text_parts.append(val)
                    if text_parts:
                        body_text = " ".join(text_parts)
            except (json.JSONDecodeError, ValueError):
                pass

        if not body_text:
            # Look for Trellis content containers
            content = soup.find("div", class_=re.compile(r"notice|content|detail|posting", re.IGNORECASE))
            if not content:
                content = soup.find("div", id=re.compile(r"notice|content|detail", re.IGNORECASE))

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
        """Return the info posting home page URL for the pipeline."""
        tsp_code = self.config["tsp_code"]
        base = self.config.get("base_url", BASE_URL)

        return [
            {
                "url": f"{base}/ptms/home/infopost/{tsp_code}",
                "tsp_code": tsp_code,
                "label": f"{self.pipeline_name}",
            }
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        tsp_code = kwargs.get("tsp_code", self.config.get("tsp_code", ""))
        base = self.config.get("base_url", BASE_URL)

        soup = BeautifulSoup(html, "html.parser")
        notices = []

        # Try extracting from notice-event-link elements with data attributes
        notice_links = soup.find_all("a", class_="notice-event-link")
        for link in notice_links:
            data_attr = link.get("data-noticesdata", "")
            if not data_attr:
                continue

            try:
                data = json.loads(data_attr)
            except (json.JSONDecodeError, ValueError):
                continue

            if not isinstance(data, dict):
                continue

            notice_id = str(
                data.get("noticeId")
                or data.get("NoticeId")
                or data.get("id")
                or ""
            )
            if not notice_id or not extract_numeric_id(notice_id):
                continue

            subject = str(
                data.get("subject")
                or data.get("Subject")
                or data.get("typeDesc")
                or clean_text(link)
                or ""
            )

            notice = {
                "notice_type": str(data.get("typeDesc", data.get("noticeType", ""))),
                "notice_subtype": "",
                "posted_datetime": str(data.get("postDateTime", data.get("date", ""))),
                "effective_datetime": str(data.get("effectiveDateTime", "")),
                "end_datetime": str(data.get("endDateTime", "")),
                "notice_identifier": notice_id,
                "notice_status": str(data.get("status", "")),
                "subject": subject,
                "response_datetime": str(data.get("responseDateTime", "")),
                "detail_url": f"{base}/ptms/home/infopost/{tsp_code}#notice-{notice_id}",
            }
            notices.append(notice)

        if notices:
            return notices

        # Fallback: try any HTML tables on the page
        tables = soup.find_all("table")
        if not tables:
            return []

        data_table = max(tables, key=lambda t: len(t.find_all("tr")))
        rows = data_table.find_all("tr")

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
                "detail_url": f"{base}/ptms/home/infopost/{tsp_code}",
            }
            notices.append(notice)

        return notices
