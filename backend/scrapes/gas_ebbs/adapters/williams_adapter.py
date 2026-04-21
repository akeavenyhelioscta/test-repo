"""
Williams 1Line source-family adapter.

Handles all pipelines hosted on the Williams 1Line platform:
  - Transco          (BUID=80)
  - Gulfstream       (BUID=205)
  - Pine Needle LNG  (BUID=82)
  - Northwest        (decommissioned/unavailable)
  - Discovery        (decommissioned/unavailable)

Williams serves notice data via JSF at:
    https://www.1line.williams.com/xhtml/notice_list.jsf?buid={BUID}&...

The JSF endpoint returns HTML tables directly (no JavaScript rendering needed).

EBB: https://www.1line.williams.com
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


JSF_BASE = "https://www.1line.williams.com/xhtml/notice_list.jsf"


@register_adapter("williams")
class WilliamsAdapter(EBBScraper):
    """Adapter for Williams 1Line EBB pages.

    Williams serves notices via a JSF endpoint that returns server-rendered
    HTML tables. Each pipeline is identified by a BUID.

    Expected table columns (8 columns):
        0: Notice Type
        1: Posted Date/Time
        2: Effective Date/Time
        3: End Date/Time
        4: Notice ID (numeric)
        5: Subject (may be linked)
        6: Response Date/Time
        7: Download link (ignored)
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Williams 1Line detail page.

        Williams detail pages are JSF-rendered with table-based layouts.
        The notice body is typically in the main content area of the page.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove scripts, styles, and navigation elements
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # Williams JSF pages often have a content panel or form with the notice text
        content = soup.find("div", id=re.compile(r"content|notice|detail", re.IGNORECASE))
        if not content:
            content = soup.find("div", class_=re.compile(r"content|notice|detail", re.IGNORECASE))

        if content:
            body_text = content.get_text(separator=" ", strip=True)
        else:
            # Fallback: find the largest table (JSF table-based layout) and
            # extract text from it
            tables = soup.find_all("table")
            longest = ""
            for table in tables:
                text = table.get_text(separator=" ", strip=True)
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
        """Return JSF URLs for critical and non-critical notices."""
        buid = self.config.get("buid", "")
        if not buid or self.config.get("disabled"):
            return []

        jsf_base = self.config.get("jsf_base_url", JSF_BASE)
        notice_types = self.config.get("notice_types", ["critical", "non-critical"])

        sources = []
        for nt in notice_types:
            critical_ind = "Y" if nt == "critical" else "N"
            url = (
                f"{jsf_base}?buid={buid}"
                f"&type=-1&type2=-1&archive=N"
                f"&critical_ind={critical_ind}"
                f"&hfSortField=posted_date&hfSortDir=DESC"
            )
            sources.append(
                {
                    "url": url,
                    "notice_type_code": nt,
                    "label": f"{self.pipeline_name} ({nt})",
                }
            )
        return sources

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "critical")
        base_url = self.config.get("base_url", "https://www.1line.williams.com")

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

            # Try column 4 first (standard layout), then scan
            notice_id = None
            notice_id_idx = None

            if len(cells) > 4:
                candidate = clean_text(cells[4])
                if extract_numeric_id(candidate):
                    notice_id = candidate
                    notice_id_idx = 4

            if notice_id is None:
                for i, cell in enumerate(cells):
                    candidate = clean_text(cell)
                    if candidate and extract_numeric_id(candidate) and candidate.strip().isdigit():
                        notice_id = candidate
                        notice_id_idx = i
                        break

            if notice_id is None:
                continue

            # Standard 7-8 column layout: Type, Posted, Effective, End, ID, Subject, Response, [Download]
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
                    detail_url = f"{base_url}{href}"
                else:
                    detail_url = f"{base_url}/{href}"

            notice = {
                "notice_type": notice_type_val,
                "notice_subtype": notice_type_code,
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
