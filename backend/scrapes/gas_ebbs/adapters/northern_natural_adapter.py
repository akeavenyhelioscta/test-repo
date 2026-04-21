"""
Northern Natural Gas source-family adapter.

Handles the Northern Natural Gas pipeline EBB:
  - Critical Notices
  - Non-Critical Notices
  - Planned Service Outage Notices

Northern Natural uses Telerik RadGrid with server-side rendered HTML tables.

EBB: https://www.northernnaturalgas.com/infopostings/
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


@register_adapter("northern_natural")
class NorthernNaturalAdapter(EBBScraper):
    """Adapter for Northern Natural Gas EBB pages.

    Northern Natural uses Telerik RadGrid to render notices server-side
    in standard HTML tables.

    Expected table columns (9 columns — cell 0 is empty filter checkbox):
        0: (empty/filter)
        1: Notice Type (e.g., "Force Majeure", "Operational Flow Order")
        2: Post Date/Time (format: "Feb 19 2026 7:56 AM")
        3: Notice Effective Date/Time
        4: Notice End Date/Time
        5: Notice ID (numeric, e.g., 102796)
        6: Notice Status (e.g., "Supersede", "Initiate")
        7: Subject
        8: Download link

    URLs:
        Critical:     /infopostings/Notices/Pages/Critical.aspx
        NonCritical:  /infopostings/Notices/Pages/NonCritical.aspx
        PlannedOutage:/infopostings/Notices/Pages/PlannedServiceOutage.aspx
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Northern Natural Gas detail page.

        NNG uses Telerik RadGrid with ASP.NET. Detail pages render
        structured notice content with fields like Location, Gas Day(s),
        Project Description, and capacity tables. The content is typically
        inside a RadGrid panel or content placeholder.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # NNG uses ASP.NET ContentPlaceHolder panels
        content = soup.find("div", id=re.compile(r"ContentPlaceHolder|MainContent|pnl", re.IGNORECASE))
        if not content:
            content = soup.find("div", class_=re.compile(r"notice|content|detail", re.IGNORECASE))
        if not content:
            # Look for RadGrid or panel content
            content = soup.find("div", class_=re.compile(r"RadGrid|rgMasterTable", re.IGNORECASE))

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
        """Return one source per configured notice page (Critical, NonCritical, etc.)."""
        base_url = self.config.get(
            "base_url",
            "https://www.northernnaturalgas.com/infopostings/Notices/Pages",
        )
        notice_pages = self.config.get(
            "notice_pages",
            [
                {"page": "Critical", "url_suffix": "Critical.aspx"},
                {"page": "NonCritical", "url_suffix": "NonCritical.aspx"},
            ],
        )

        sources = []
        for page_config in notice_pages:
            page_name = page_config["page"]
            url_suffix = page_config["url_suffix"]
            url = f"{base_url}/{url_suffix}"
            sources.append(
                {
                    "url": url,
                    "notice_page": page_name,
                    "label": f"{self.pipeline_name} ({page_name})",
                }
            )
        return sources

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_page = kwargs.get("notice_page", "Critical")

        soup = BeautifulSoup(html, "html.parser")

        # Northern Natural uses Telerik RadGrid — look for the grid table.
        # The RadGrid table typically has class "rgMasterTable" or is the
        # largest table on the page.
        tables = soup.find_all("table")
        if not tables:
            return []

        # Try to find RadGrid table by class first
        data_table = None
        for table in tables:
            classes = table.get("class", [])
            if any("rgMasterTable" in c for c in classes):
                data_table = table
                break
            if any("RadGrid" in c for c in classes):
                data_table = table
                break

        # Fallback: pick the table with the most rows
        if data_table is None:
            max_rows = 0
            for table in tables:
                rows = table.find_all("tr")
                if len(rows) > max_rows:
                    max_rows = len(rows)
                    data_table = table

        if not data_table:
            return []

        rows = data_table.find_all("tr")
        if len(rows) < 2:
            return []

        notices = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            # Column 5 is Notice ID (column 0 is an empty filter checkbox)
            notice_id_text = clean_text(cells[5]) if len(cells) > 5 else ""
            notice_id = extract_numeric_id(notice_id_text)
            if not notice_id:
                # Scan for a numeric cell as fallback
                for i, cell in enumerate(cells):
                    candidate = clean_text(cell)
                    if candidate and candidate.strip().isdigit() and len(candidate.strip()) >= 4:
                        notice_id = candidate
                        break
                if not notice_id:
                    continue

            # Extract fields from the 9-column layout (cell 0 is empty)
            notice_type_val = clean_text(cells[1])
            posted_dt = clean_text(cells[2])
            effective_dt = clean_text(cells[3])
            end_dt = clean_text(cells[4])
            notice_status = clean_text(cells[6]) if len(cells) > 6 else ""

            # Subject is in column 7
            subject_text = ""
            if len(cells) > 7:
                subject_text = clean_text(cells[7])

            # Download link is in column 8
            detail_url = ""
            if len(cells) > 8:
                download_cell = cells[8]
                link = download_cell.find("a")
                if link and link.get("href"):
                    href = link["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = (
                            f"https://www.northernnaturalgas.com{href}"
                        )
                    else:
                        detail_url = (
                            f"https://www.northernnaturalgas.com/"
                            f"infopostings/Notices/Pages/{href}"
                        )

            notice = {
                "notice_type": notice_type_val,
                "notice_subtype": notice_page,
                "posted_datetime": posted_dt,
                "effective_datetime": effective_dt,
                "end_datetime": end_dt,
                "notice_identifier": notice_id,
                "notice_status": notice_status,
                "subject": subject_text,
                "response_datetime": "",
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices
