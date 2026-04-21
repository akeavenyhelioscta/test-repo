"""
GasNom source-family adapter.

Handles all pipelines hosted on gasnom.com (6 pipelines).
Standard HTML table parsing.

Pipelines: Southern Pines, Cameron Interstate, Golden Pass,
           Golden Triangle, Mississippi Hub, LA Storage

EBB: http://www.gasnom.com/
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


BASE_URL = "http://www.gasnom.com"


@register_adapter("gasnom")
class GasNomAdapter(EBBScraper):
    """Adapter for GasNom-hosted pipeline EBB pages.

    GasNom serves pipeline information at /ip/{path}/.
    Notice listings are typically in HTML tables.

    Expected table columns:
        0: Notice Type
        1: Posted Date/Time
        2: Effective Date/Time
        3: End Date/Time
        4: Notice ID (numeric)
        5: Subject (may be linked)
        6: Response Date/Time (if present)
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse GasNom detail page.

        GasNom detail pages (notices.cfm or notice detail links) render
        notice content in standard HTML tables and divs. The adapter
        extracts text from the main content area.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # GasNom uses ColdFusion (notices.cfm) — look for content containers
        content = soup.find("div", class_=re.compile(r"notice|content|detail|body", re.IGNORECASE))
        if not content:
            content = soup.find("div", id=re.compile(r"notice|content|detail|body", re.IGNORECASE))

        if content:
            body_text = content.get_text(separator=" ", strip=True)
        else:
            # Fallback: find the largest table (GasNom is table-based)
            tables = soup.find_all("table")
            longest = ""
            for table in tables:
                text = table.get_text(separator=" ", strip=True)
                if len(text) > len(longest):
                    longest = text
            if len(longest) > 100:
                body_text = longest
            else:
                # Last resort: full page text
                body_text = soup.get_text(separator=" ", strip=True)

        body_text = " ".join(body_text.split())

        extraction = outage_extractor.extract_outage(
            subject=notice.get("subject", ""),
            detail_text=body_text,
        )
        extraction["detail_text"] = body_text[:5000]
        return extraction

    def _get_listing_sources(self) -> list[dict]:
        """Return listing URLs for critical and non-critical notices.

        GasNom serves notices at /ip/{path}/notices.cfm?type=1 (Critical)
        and /ip/{path}/notices.cfm?type=2 (Non-Critical).
        """
        path = self.config["path"]
        base = self.config.get("base_url", BASE_URL)
        notice_types = self.config.get("notice_types", [
            {"code": "1", "label": "Critical"},
            {"code": "2", "label": "Non-Critical"},
        ])

        return [
            {
                "url": f"{base}/ip/{path}/notices.cfm?type={nt['code']}",
                "label": f"{self.pipeline_name} ({nt['label']})",
            }
            for nt in notice_types
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        base = self.config.get("base_url", BASE_URL)
        path = self.config["path"]

        soup = BeautifulSoup(html, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            return []

        # Find the data table — pick the table with the most rows
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
            if len(cells) < 5:
                continue

            # Scan for a cell containing a numeric notice ID
            notice_id = None
            notice_id_idx = None

            # Try column 4 first (standard layout)
            if len(cells) > 4:
                candidate = clean_text(cells[4])
                if candidate and extract_numeric_id(candidate) and candidate.strip().isdigit():
                    notice_id = candidate
                    notice_id_idx = 4

            # Fallback: scan all cells
            if notice_id is None:
                for i, cell in enumerate(cells):
                    candidate = clean_text(cell)
                    if candidate and candidate.strip().isdigit():
                        notice_id = candidate
                        notice_id_idx = i
                        break

            if notice_id is None:
                continue

            # Extract fields based on detected layout
            if notice_id_idx == 4 and len(cells) >= 6:
                notice_type_val = clean_text(cells[0])
                posted_dt = clean_text(cells[1])
                effective_dt = clean_text(cells[2])
                end_dt = clean_text(cells[3])
                subject_cell = cells[5]
                response_dt = clean_text(cells[6]) if len(cells) >= 7 else ""
            else:
                notice_type_val = clean_text(cells[0]) if len(cells) > 0 else ""
                posted_dt = clean_text(cells[1]) if len(cells) > 1 else ""
                effective_dt = clean_text(cells[2]) if len(cells) > 2 else ""
                end_dt = clean_text(cells[3]) if len(cells) > 3 else ""
                subj_idx = notice_id_idx + 1 if notice_id_idx + 1 < len(cells) else notice_id_idx
                subject_cell = cells[subj_idx]
                resp_idx = notice_id_idx + 2
                response_dt = clean_text(cells[resp_idx]) if resp_idx < len(cells) else ""

            subject_text = clean_text(subject_cell)
            subject_link = subject_cell.find("a")

            detail_url = ""
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = f"{base}{href}"
                else:
                    detail_url = f"{base}/ip/{path}/{href}"

            notice = {
                "notice_type": notice_type_val,
                "notice_subtype": "",
                "posted_datetime": posted_dt,
                "effective_datetime": effective_dt,
                "end_datetime": end_dt,
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": subject_text,
                "response_datetime": response_dt,
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices
