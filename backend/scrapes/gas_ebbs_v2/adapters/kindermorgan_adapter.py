"""
Kinder Morgan source-family adapter.

Handles all pipelines hosted on pipeline2.kindermorgan.com (19 pipelines).
Parses the Infragistics WebDataGrid (DGNotices) which renders server-side
HTML table rows containing notice data.

Supports both Critical (type=C) and Non-Critical (type=N) notice pages.

EBB: https://pipeline2.kindermorgan.com/Notices/
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs_v2.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs_v2.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs_v2 import outage_extractor


BASE_URL = "https://pipeline2.kindermorgan.com"

# Map the type query-param codes to human-readable labels
NOTICE_TYPE_LABELS = {
    "C": "Critical",
    "N": "Non-Critical",
}


@register_adapter("kindermorgan")
class KinderMorganAdapter(EBBScraper):
    """Adapter for Kinder Morgan pipeline EBB pages.

    The Kinder Morgan EBB uses an Infragistics WebDataGrid that renders
    notice data as standard HTML table rows scattered across the page.
    The DGNotices ID is a hidden input (client state), not the table itself.

    Each notice row has 9 cells:
        0: Notice Type Category (e.g. "PIPELINE CONDITIONS")
        1: Notice Type Subcategory (e.g. "CURRENT PIPELINE CONDITIONS")
        2: Post Date/Time (format: MM/dd/yyyy h:mm:ssTT)
        3: Notice Effective Date/Time
        4: Notice End Date/Time
        5: Notice ID (numeric, e.g. 399881)
        6: Subject (linked text with download link)
        7: (empty or checkbox)
        8: (empty or checkbox)

    Detail URL pattern:
        /Notices/NoticeDetail.aspx?code={pipe_code}&NoticeId={notice_id}
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Kinder Morgan detail page (NoticeDetail.aspx).

        KM detail pages render the notice body in a panel with
        structured fields and a text body area.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style"]):
            tag.decompose()

        # KM detail pages often have a content panel or span with the notice text
        body_text = ""
        content = soup.find("div", id=re.compile(r"ContentPlaceHolder|pnlNotice", re.IGNORECASE))
        if content:
            body_text = content.get_text(separator=" ", strip=True)
        else:
            # Fallback: find the largest text block on the page
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
        """Return one source per configured notice type (C, N)."""
        pipe_code = self.config["pipe_code"]
        notice_types = self.config.get("notice_types", ["C", "N"])
        base = self.config.get("base_url", BASE_URL)

        return [
            {
                "url": (
                    f"{base}/Notices/Notices.aspx"
                    f"?type={nt}&code={pipe_code}"
                ),
                "notice_type_code": nt,
                "label": (
                    f"{self.pipeline_name} "
                    f"({NOTICE_TYPE_LABELS.get(nt, nt)})"
                ),
            }
            for nt in notice_types
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "C")
        pipe_code = self.config["pipe_code"]
        base = self.config.get("base_url", BASE_URL)

        soup = BeautifulSoup(html, "html.parser")

        # Infragistics renders notice rows as <tr> with 9 <td> cells.
        # The grid's DGNotices ID is just a hidden input, not a container.
        # Scan ALL <tr> elements in the page for notice data rows.
        rows = soup.find_all("tr")
        notices = []

        for row in rows:
            cells = row.find_all("td")

            # Notice rows have 9 cells
            if len(cells) < 7:
                continue

            # Cell 5 is Notice ID — must be a 5-6 digit number
            notice_id_text = clean_text(cells[5])
            if not notice_id_text or not notice_id_text.strip().isdigit():
                continue
            notice_id = notice_id_text.strip()
            if len(notice_id) < 4:
                continue

            # Cell 6 is Subject — extract link for detail URL
            subject_cell = cells[6]
            subject_text = clean_text(subject_cell)
            subject_link = subject_cell.find("a")

            detail_url = ""
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = f"{base}{href}"

            if not detail_url:
                detail_url = (
                    f"{base}/Notices/NoticeDetail.aspx"
                    f"?code={pipe_code}&NoticeId={notice_id}"
                )

            notice = {
                "notice_type": clean_text(cells[0]),
                "notice_subtype": clean_text(cells[1]),
                "posted_datetime": clean_text(cells[2]),
                "effective_datetime": clean_text(cells[3]),
                "end_datetime": clean_text(cells[4]),
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": subject_text,
                "response_datetime": "",
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices
