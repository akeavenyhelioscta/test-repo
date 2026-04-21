"""
Quorum (MyQuorumCloud) source-family adapter.

Handles all pipelines hosted on web-prd.myquorumcloud.com (7 pipelines).
Quorum uses a Kendo Grid that may serve data via a JSON API endpoint
or render server-side HTML. The adapter tries JSON parsing first, then
falls back to HTML table parsing.

Supports Critical (CriticalNoticeCode=Y) and Non-Critical (CriticalNoticeCode=N).

App codes: BBTPA1IPWS (BBT pipelines), HPEPA1IPWS (HPE pipelines)

EBB: https://web-prd.myquorumcloud.com/
"""

import json
import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs_v2.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs_v2.ebb_utils import clean_text, extract_numeric_id, DEFAULT_HEADERS
from backend.scrapes.gas_ebbs_v2 import outage_extractor


BASE_URL = "https://web-prd.myquorumcloud.com"

# Map CriticalNoticeCode values to labels
NOTICE_TYPE_LABELS = {
    "Y": "Critical",
    "N": "Non-Critical",
}

# Kendo Grid column field names to canonical field mapping
KENDO_FIELD_MAP = {
    "NoticeSubType": "notice_type",
    "noticeSubType": "notice_type",
    "notice_sub_type": "notice_type",
    "PostDatetime": "posted_datetime",
    "postDatetime": "posted_datetime",
    "post_datetime": "posted_datetime",
    "NoticeEffDatetime": "effective_datetime",
    "noticeEffDatetime": "effective_datetime",
    "notice_eff_datetime": "effective_datetime",
    "NoticeEndDatetime": "end_datetime",
    "noticeEndDatetime": "end_datetime",
    "notice_end_datetime": "end_datetime",
    "NoticeId": "notice_identifier",
    "noticeId": "notice_identifier",
    "notice_id": "notice_identifier",
    "NoticeSubject": "subject",
    "noticeSubject": "subject",
    "notice_subject": "subject",
    "ResponseDatetime": "response_datetime",
    "responseDatetime": "response_datetime",
    "response_datetime": "response_datetime",
}


@register_adapter("quorum")
class QuorumAdapter(EBBScraper):
    """Adapter for Quorum (MyQuorumCloud) EBB pages.

    Quorum uses a Kendo Grid for notice display. The grid may have a
    JSON data endpoint, or it may render server-side HTML.

    Kendo Grid columns:
        0: NoticeSubType (Notice Type) — 75px
        1: PostDatetime (Posted Date/Time) — M/d/yyyy hh:mm:ss tt
        2: NoticeEffDatetime (Notice Eff Date/Time)
        3: NoticeEndDatetime (Notice End Date/Time)
        4: NoticeId (Notice Identifier) — numeric
        5: NoticeSubject (Subject)
        6: ResponseDatetime (Notice Response Date/Time)
        7: Action (view details link)

    Detail URL pattern:
        /{app_code}/NoticePosting/Detail?noticeId={notice_id}&tspno={tspno}
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse Quorum (MyQuorumCloud) detail page.

        Quorum detail pages (NoticePosting/Detail) render the notice
        body in an HTML page. The Kendo UI framework wraps the content
        in structured div containers.
        """
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # Quorum detail pages use a content panel or notice body area
        content = soup.find("div", class_=re.compile(r"notice-detail|notice-body|content|detail-content", re.IGNORECASE))
        if not content:
            content = soup.find("div", id=re.compile(r"notice|content|detail", re.IGNORECASE))

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

    def _get_listing_sources(self) -> list[dict]:
        """Return one source per configured notice type (Y=Critical, N=Non-Critical)."""
        app_code = self.config["app_code"]
        tspno = self.config["tspno"]
        notice_types = self.config.get("notice_types", ["Y", "N"])
        base = self.config.get("base_url", BASE_URL)

        return [
            {
                "url": (
                    f"{base}/{app_code}/NoticePosting"
                    f"?tspno={tspno}&CriticalNoticeCode={nt}"
                ),
                "notice_type_code": nt,
                "app_code": app_code,
                "tspno": tspno,
                "label": (
                    f"{self.pipeline_name} "
                    f"({NOTICE_TYPE_LABELS.get(nt, nt)})"
                ),
            }
            for nt in notice_types
        ]

    def _pull(self, url: str = "") -> str:
        """Fetch from Quorum, trying JSON API first then HTML.

        Kendo Grids often have a read endpoint that returns JSON data.
        Common pattern: POST to the same URL with grid parameters.
        """
        import requests

        target = url or self.listing_url

        # Try fetching with JSON accept header (Kendo data endpoint)
        api_headers = {
            **DEFAULT_HEADERS,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }

        # Try POST request (Kendo grid data source pattern)
        try:
            response = requests.post(
                target,
                headers=api_headers,
                data={
                    "sort": "",
                    "page": "1",
                    "pageSize": "500",
                    "group": "",
                    "filter": "",
                },
                timeout=30,
            )
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "")
                if "json" in content_type or "javascript" in content_type:
                    return response.text
                try:
                    json.loads(response.text)
                    return response.text
                except (json.JSONDecodeError, ValueError):
                    pass
        except requests.RequestException:
            pass

        # Fallback: standard GET for HTML page
        response = requests.get(target, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        return response.text

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        notice_type_code = kwargs.get("notice_type_code", "Y")
        app_code = kwargs.get("app_code", self.config.get("app_code", ""))
        tspno = kwargs.get("tspno", self.config.get("tspno", ""))
        base = self.config.get("base_url", BASE_URL)

        # Try parsing as JSON first (Kendo grid data)
        notices = self._try_parse_json(html, notice_type_code, app_code, tspno, base)
        if notices is not None:
            return notices

        # Fallback: parse as HTML
        return self._parse_html(html, notice_type_code, app_code, tspno, base)

    def _try_parse_json(
        self,
        text: str,
        notice_type_code: str,
        app_code: str,
        tspno: str,
        base: str,
    ) -> list[dict] | None:
        """Attempt to parse the response as Kendo Grid JSON data.

        Kendo grids typically return: {"Data": [...], "Total": N}
        or just a list of records.

        Returns a list of notice dicts on success, or None if not JSON.
        """
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return None

        # Handle Kendo response structure: {"Data": [...], "Total": N}
        records = None
        if isinstance(data, dict):
            for key in ("Data", "data", "d", "Results", "results", "notices"):
                if key in data and isinstance(data[key], list):
                    records = data[key]
                    break
            if records is None:
                return None
        elif isinstance(data, list):
            records = data
        else:
            return None

        if not records:
            return None

        # Verify first record looks like notice data
        first = records[0]
        if not isinstance(first, dict):
            return None

        if not any(k in first for k in KENDO_FIELD_MAP):
            return None

        notices = []
        for item in records:
            if not isinstance(item, dict):
                continue

            # Map Kendo field names to canonical names
            mapped = {}
            for kendo_field, canonical_field in KENDO_FIELD_MAP.items():
                if kendo_field in item:
                    mapped[canonical_field] = str(item[kendo_field] or "")

            notice_id = mapped.get("notice_identifier", "")
            if not notice_id:
                continue

            detail_url = (
                f"{base}/{app_code}/NoticePosting/Detail"
                f"?noticeId={notice_id}&tspno={tspno}"
            )

            notice = {
                "notice_type": mapped.get("notice_type", ""),
                "notice_subtype": "",
                "posted_datetime": mapped.get("posted_datetime", ""),
                "effective_datetime": mapped.get("effective_datetime", ""),
                "end_datetime": mapped.get("end_datetime", ""),
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": mapped.get("subject", ""),
                "response_datetime": mapped.get("response_datetime", ""),
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices if notices else None

    def _parse_html(
        self,
        html: str,
        notice_type_code: str,
        app_code: str,
        tspno: str,
        base: str,
    ) -> list[dict]:
        """Fallback HTML parser for Quorum Kendo Grid pages.

        The Kendo Grid renders as a standard HTML table with specific
        CSS classes. Columns follow the order defined in the grid config.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Kendo grids use class "k-grid" or "k-grid-content"
        grid = soup.find(class_=re.compile(r"k-grid"))
        if grid:
            data_table = grid.find("table")
        else:
            # Fallback: find the largest table on the page
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

            # Cell 4 is NoticeId — must be numeric
            notice_id_text = clean_text(cells[4])
            notice_id = extract_numeric_id(notice_id_text)
            if not notice_id:
                continue

            # Cell 5 is NoticeSubject
            subject_cell = cells[5]
            subject_text = clean_text(subject_cell)

            # Check for detail link in subject or action column
            detail_url = ""
            subject_link = subject_cell.find("a")
            if subject_link and subject_link.get("href"):
                href = subject_link["href"]
                if href.startswith("http"):
                    detail_url = href
                elif href.startswith("/"):
                    detail_url = f"{base}{href}"
                else:
                    detail_url = f"{base}/{href}"

            # Also check the action column (last cell) for view link
            if not detail_url and len(cells) > 7:
                action_cell = cells[7]
                action_link = action_cell.find("a")
                if action_link and action_link.get("href"):
                    href = action_link["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        detail_url = f"{base}{href}"
                    else:
                        detail_url = f"{base}/{href}"

            if not detail_url:
                detail_url = (
                    f"{base}/{app_code}/NoticePosting/Detail"
                    f"?noticeId={notice_id}&tspno={tspno}"
                )

            # Response datetime is cell 6 if present
            response_dt = ""
            if len(cells) > 6:
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
