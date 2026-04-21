"""
TC Plus source-family adapter.

Handles all pipelines hosted on www.tcplus.com (4 pipelines).
Standard HTML table parsing with CSS class-based cell identification.
Supports Critical and NonCritical notice pages.

Pipelines: GTN, Great Lakes, Tuscarora, North Baja

EBB: http://www.tcplus.com/
"""

import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


BASE_URL = "http://www.tcplus.com"

# Map notice type codes to labels
NOTICE_TYPE_LABELS = {
    "Critical": "Critical",
    "NonCritical": "Non-Critical",
}

# CSS class to field name mapping for TC Plus table cells
CSS_CLASS_FIELD_MAP = {
    "notice-type": "notice_type",
    "post-date": "posted_datetime",
    "eff-date": "effective_datetime",
    "id": "notice_identifier",
    "subject": "subject",
    "resp-date": "response_datetime",
    "prior-id": "prior_notice_id",
    "status": "notice_status",
    "has-attachments": "has_attachments",
}


@register_adapter("tcplus")
class TCPlusAdapter(EBBScraper):
    """Adapter for TC Plus pipeline EBB pages.

    TC Plus serves notices in an HTML table with well-defined CSS classes
    on each cell (notice-type, post-date, eff-date, id, subject, resp-date,
    prior-id, status, has-attachments).

    Each notice row has a ``data-noticeid`` attribute on the <tr> element.

    Table columns (10):
        0: Notice Type           (class: notice-type)
        1: Posting Date/Time     (class: post-date)
        2: Notice Effective Date/Time (class: eff-date)
        3: Notice ID             (class: id)
        4: Subject               (class: subject)
        5: Response Date/Time    (class: resp-date)
        6: Prior Notice          (class: prior-id)
        7: Notice Status         (class: status)
        8: Attachments           (class: has-attachments)
        9: Actions               (view link)

    Detail URL pattern:
        /{path}/Notice/View/{notice_id}
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse TC Plus detail page (Notice/View/{id}).

        TC Plus detail pages render the notice body in an HTML page
        with well-defined CSS classes. The main content is typically
        in a notice-detail or content container.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # TC Plus uses CSS classes for structure — look for notice content
        content = soup.find("div", class_=re.compile(r"notice-detail|notice-content|content-area|detail", re.IGNORECASE))
        if not content:
            content = soup.find("div", id=re.compile(r"notice|content|detail", re.IGNORECASE))
        if not content:
            # Look for the main element
            content = soup.find("main") or soup.find("article")

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
        """Return one source per configured notice type (Critical, NonCritical)."""
        path = self.config["path"]
        notice_types = self.config.get("notice_types", ["Critical", "NonCritical"])
        base = self.config.get("base_url", BASE_URL)

        return [
            {
                "url": f"{base}/{path}/Notice/{nt}",
                "notice_type_code": nt,
                "path": path,
                "label": (
                    f"{self.pipeline_name} "
                    f"({NOTICE_TYPE_LABELS.get(nt, nt)})"
                ),
            }
            for nt in notice_types
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "Critical")
        path = kwargs.get("path", self.config.get("path", ""))
        base = self.config.get("base_url", BASE_URL)

        soup = BeautifulSoup(html, "html.parser")

        # Find the data table — TC Plus uses a standard HTML table
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
            if len(cells) < 5:
                continue

            # Try CSS class-based extraction first
            notice = self._extract_by_css_classes(row, cells, path, base)

            if notice is None:
                # Fallback: positional cell extraction
                notice = self._extract_by_position(
                    row, cells, notice_type_code, path, base
                )

            if notice is not None:
                notices.append(notice)

        return notices

    def _extract_by_css_classes(
        self, row, cells, path: str, base: str
    ) -> dict | None:
        """Extract notice data using CSS classes on table cells.

        Returns a notice dict if CSS classes are found, None otherwise.
        """
        fields = {}

        for cell in cells:
            cell_classes = cell.get("class", [])
            for css_class in cell_classes:
                if css_class in CSS_CLASS_FIELD_MAP:
                    field_name = CSS_CLASS_FIELD_MAP[css_class]
                    fields[field_name] = clean_text(cell)
                    # Store the cell itself for link extraction
                    if css_class == "subject":
                        fields["_subject_cell"] = cell

        # Must have at least notice_identifier or subject to be valid
        if "notice_identifier" not in fields and "subject" not in fields:
            return None

        notice_id = fields.get("notice_identifier", "")

        # Also try data-noticeid attribute on the row
        if not notice_id:
            notice_id = row.get("data-noticeid", "")

        if not notice_id or not extract_numeric_id(str(notice_id)):
            return None

        # Extract detail URL from subject link or actions column
        detail_url = ""
        subject_cell = fields.pop("_subject_cell", None)
        if subject_cell:
            link = subject_cell.find("a")
            if link and link.get("href"):
                href = link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = f"{base}{href}"
                else:
                    detail_url = f"{base}/{href}"

        # Check actions column for view link
        if not detail_url:
            for cell in cells:
                link = cell.find("a")
                if link and link.get("href") and "view" in (link.get("href", "").lower()):
                    href = link["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = f"{base}{href}"
                    else:
                        detail_url = f"{base}/{href}"
                    break

        if not detail_url:
            detail_url = f"{base}/{path}/Notice/View/{notice_id}"

        return {
            "notice_type": fields.get("notice_type", ""),
            "notice_subtype": "",
            "posted_datetime": fields.get("posted_datetime", ""),
            "effective_datetime": fields.get("effective_datetime", ""),
            "end_datetime": "",
            "notice_identifier": str(notice_id),
            "notice_status": fields.get("notice_status", ""),
            "subject": fields.get("subject", ""),
            "response_datetime": fields.get("response_datetime", ""),
            "detail_url": detail_url,
        }

    def _extract_by_position(
        self,
        row,
        cells,
        notice_type_code: str,
        path: str,
        base: str,
    ) -> dict | None:
        """Fallback: extract notice data by cell position.

        TC Plus tables have 10 columns:
            0: Notice Type
            1: Posting Date/Time
            2: Notice Effective Date/Time
            3: Notice ID
            4: Subject
            5: Response Date/Time
            6: Prior Notice
            7: Notice Status
            8: Attachments
            9: Actions
        """
        if len(cells) < 5:
            return None

        # Try to get notice_id from data-noticeid attribute first
        notice_id = row.get("data-noticeid", "")
        if not notice_id and len(cells) > 3:
            notice_id = clean_text(cells[3])

        if not notice_id or not extract_numeric_id(str(notice_id)):
            return None

        # Extract subject and detail URL
        subject_text = ""
        detail_url = ""

        if len(cells) > 4:
            subject_cell = cells[4]
            subject_text = clean_text(subject_cell)
            subject_link = subject_cell.find("a")
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = f"{base}{href}"
                else:
                    detail_url = f"{base}/{href}"

        if not detail_url:
            detail_url = f"{base}/{path}/Notice/View/{notice_id}"

        return {
            "notice_type": clean_text(cells[0]) if len(cells) > 0 else "",
            "notice_subtype": "",
            "posted_datetime": clean_text(cells[1]) if len(cells) > 1 else "",
            "effective_datetime": clean_text(cells[2]) if len(cells) > 2 else "",
            "end_datetime": "",
            "notice_identifier": str(notice_id),
            "notice_status": clean_text(cells[7]) if len(cells) > 7 else "",
            "subject": subject_text,
            "response_datetime": clean_text(cells[5]) if len(cells) > 5 else "",
            "detail_url": detail_url,
        }
