"""
Enbridge InfoPost source-family adapter.

Handles all pipelines hosted on infopost.enbridge.com (24+ pipelines).
Standard HTML table parsing — no session or JavaScript required.
Supports multiple notice types per pipeline (CRI, NON, OUT, etc.).

EBB: https://infopost.enbridge.com/infopost/
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


BASE_URL = "https://infopost.enbridge.com/infopost"


@register_adapter("enbridge")
class EnbridgeAdapter(EBBScraper):
    """Adapter for Enbridge InfoPost EBB pages.

    Enbridge serves notices in a standard HTML table.

    Critical notices (type=CRI) have 7 columns:
        0: notice_type
        1: posted_datetime
        2: effective_datetime
        3: end_datetime
        4: notice_identifier (numeric)
        5: subject (linked)
        6: response_datetime

    Non-critical notices (type=NON) have 6 columns:
        0: notice_type
        1: posted_datetime
        2: effective_datetime
        3: end_datetime
        4: notice_identifier (numeric)
        5: subject (linked)

    Detail URL pattern:
        NoticeListDetail.asp?strKey1={id}&type={type_code}&Embed=2&pipe={pipe_code}
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Enbridge InfoPost detail page.

        Enbridge detail pages (NoticeListDetail.asp) render the notice body
        in a structured HTML layout with tables for metadata.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style"]):
            tag.decompose()

        # Enbridge detail pages use tables — look for the largest text block
        body_text = ""
        # Try <td> cells that contain the main notice body
        tds = soup.find_all("td")
        longest = ""
        for td in tds:
            text = td.get_text(separator=" ", strip=True)
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
        """Return one source per configured notice type (CRI, NON, etc.)."""
        pipe_code = self.config["pipe_code"]
        notice_types = self.config.get("notice_types", ["CRI", "NON"])
        base = self.config.get("base_url", BASE_URL)

        return [
            {
                "url": f"{base}/NoticesList.asp?pipe={pipe_code}&type={nt}",
                "notice_type_code": nt,
                "label": f"{self.pipeline_name} ({nt})",
            }
            for nt in notice_types
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "CRI")
        pipe_code = self.config["pipe_code"]
        base = self.config.get("base_url", BASE_URL)

        soup = BeautifulSoup(html, "html.parser")

        # Find the data table — Enbridge pages use standard <table> elements.
        # The notice table is typically the largest table on the page.
        tables = soup.find_all("table")
        if not tables:
            return []

        data_table = None
        max_rows = 0
        for table in tables:
            rows = table.find_all("tr")
            if len(rows) > max_rows:
                max_rows = len(rows)
                data_table = table

        if not data_table or max_rows < 2:
            return []

        rows = data_table.find_all("tr")
        notices = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            # Cell 4 is notice_identifier — must be numeric
            notice_id = clean_text(cells[4])
            if not extract_numeric_id(notice_id):
                continue

            # Subject is in cell 5; extract link for detail URL
            subject_cell = cells[5]
            subject_text = clean_text(subject_cell)
            subject_link = subject_cell.find("a")

            detail_url = ""
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                else:
                    detail_url = f"{base}/{href}"
            if not detail_url:
                detail_url = (
                    f"{base}/NoticeListDetail.asp"
                    f"?strKey1={notice_id}&type={notice_type_code}"
                    f"&Embed=2&pipe={pipe_code}"
                )

            # Response datetime is cell 6 (CRI only, 7 cells)
            response_dt = ""
            if len(cells) >= 7:
                response_dt = clean_text(cells[6])

            notice = {
                "notice_type": clean_text(cells[0]),
                "notice_subtype": "",
                "posted_datetime": clean_text(cells[1]),
                "effective_datetime": clean_text(cells[2]),
                "end_datetime": clean_text(cells[3]),
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": subject_text,
                "response_datetime": response_dt,
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices
