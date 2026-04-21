"""
Energy Transfer source-family adapter.

Handles all pipelines hosted on the Energy Transfer platform (Quorum-based):
  - Florida Gas        (fgttransfer.energytransfer.com)
  - Panhandle Eastern  (peplmessenger.energytransfer.com)
  - Transwestern       (twtransfer.energytransfer.com)
  - Trunkline Gas      (tgcmessenger.energytransfer.com)
  - Sea Robin          (sermessenger.energytransfer.com)
  - Tiger              (tigertransfer.energytransfer.com)
  - Fayetteville Exp   (feptransfer.energytransfer.com)
  - Trunkline LNG      (tlngmessenger.energytransfer.com)
  - SW Gas Storage     (swgsmessenger.energytransfer.com)

Each pipeline has its own subdomain and code:
    https://{subdomain}.energytransfer.com/ipost/{code}/

EBB: https://energytransfer.com
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs_v2.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs_v2.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs_v2 import outage_extractor


@register_adapter("energytransfer")
class EnergyTransferAdapter(EBBScraper):
    """Adapter for Energy Transfer (Quorum-based) EBB pages.

    Energy Transfer pipelines use a Quorum platform. The notice listing
    is at ``/ipost/{code}/notices/search`` with query parameters for
    notice type (reqType=CRI for critical, reqType=NON for non-critical).

    Expected table columns (7 columns):
        0: Notice Type
        1: Posted Date/Time
        2: Effective Date/Time
        3: End Date/Time
        4: Notice ID (numeric)
        5: Subject (may be linked)
        6: Response Date/Time

    Non-critical notices may omit the Response Date/Time column (6 columns).
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Energy Transfer (Quorum-based) detail page.

        ET detail pages use a div-based layout with the notice body
        rendered in a content panel. The structure mirrors the Quorum
        platform used across all ET subsidiaries.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove scripts, styles, and navigation
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # ET/Quorum detail pages often use a content div or main area
        content = soup.find("div", class_=re.compile(r"notice|content|detail|body", re.IGNORECASE))
        if not content:
            content = soup.find("div", id=re.compile(r"notice|content|detail|body", re.IGNORECASE))

        if content:
            body_text = content.get_text(separator=" ", strip=True)
        else:
            # Fallback: find the largest text block among divs
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

    def _get_base_url(self) -> str:
        """Build the base URL for a pipeline from subdomain and code."""
        subdomain = self.config["subdomain"]
        pipe_code = self.config["pipe_code"]
        return f"https://{subdomain}.energytransfer.com/ipost/{pipe_code}"

    def _get_listing_sources(self) -> list[dict]:
        """Return one source per configured notice type (CRI, NON, etc.)."""
        base = self._get_base_url()
        notice_types = self.config.get("notice_types", ["CRI", "NON"])

        sources = []
        for nt in notice_types:
            url = f"{base}/notices/search?reqType={nt}"
            sources.append(
                {
                    "url": url,
                    "notice_type_code": nt,
                    "label": f"{self.pipeline_name} ({nt})",
                }
            )
        return sources

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "CRI")
        base = self._get_base_url()

        soup = BeautifulSoup(html, "html.parser")

        # Find all tables on the page
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

            # Attempt to detect the notice ID column.
            # Standard layout: column 4 is Notice ID (numeric).
            notice_id = None
            notice_id_idx = None

            if len(cells) > 4:
                candidate = clean_text(cells[4])
                if extract_numeric_id(candidate):
                    notice_id = candidate
                    notice_id_idx = 4

            # Fallback: scan cells for a purely numeric value
            if notice_id is None:
                for i, cell in enumerate(cells):
                    candidate = clean_text(cell)
                    if candidate and candidate.strip().isdigit():
                        notice_id = candidate
                        notice_id_idx = i
                        break

            if notice_id is None:
                continue

            # Build field mapping based on detected layout
            if notice_id_idx == 4 and len(cells) >= 6:
                notice_type_val = clean_text(cells[0])
                posted_dt = clean_text(cells[1])
                effective_dt = clean_text(cells[2])
                end_dt = clean_text(cells[3])
                subject_cell = cells[5]
                response_dt = clean_text(cells[6]) if len(cells) >= 7 else ""
            else:
                # Non-standard layout — extract what we can
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

            # Build detail URL from link or construct one
            detail_url = ""
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    # Absolute path — prepend the host
                    subdomain = self.config["subdomain"]
                    detail_url = f"https://{subdomain}.energytransfer.com{href}"
                else:
                    detail_url = f"{base}/{href}"

            if not detail_url:
                # Construct a plausible detail URL
                detail_url = (
                    f"{base}/notices/detail"
                    f"?reqType={notice_type_code}&noticeId={notice_id}"
                )

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
