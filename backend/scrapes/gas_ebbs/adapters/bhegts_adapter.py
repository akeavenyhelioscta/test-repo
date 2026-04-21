"""
BHEGTS (Dominion) source-family adapter.

Handles pipelines hosted on the BHEGTS/Dominion InfoPost platform:
  - Carolina Gas       (cgt)
  - Cove Point         (cpl)
  - Eastern Gas        (egts)

The platform serves notice data as JSON embedded in the page content.
A ``postings`` array contains notice objects with fields like noticeId,
subject, postedDate, effectiveDate, etc.

EBB: https://infopost.bhegts.com
"""

import json
import re

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id
from backend.scrapes.gas_ebbs import outage_extractor


@register_adapter("bhegts")
class BHEGTSAdapter(EBBScraper):
    """Adapter for BHEGTS/Dominion InfoPost EBB pages.

    BHEGTS serves notices as JSON postings data. Each notice object contains:
        - id, noticeId, revision
        - category, subcategory (e.g., "Critical", "Non-Critical")
        - subject, naesbNoticeTypeDescription, tspDefinedNoticeTypeDescription
        - postedDate, effectiveDate, responseDate
        - content array with PDF documents

    URL patterns:
        API: /api/v1/{code}/postings?category=notices&subcategory={subcategory}
        Doc: https://infopost.bhegts.com/docs/{code}/postings/{noticeId}/{revision}/{code}-{noticeId}.pdf
    """

    def _parse_detail(self, html: str, notice: dict) -> dict:
        """Parse BHEGTS/Dominion detail page.

        BHEGTS detail URLs typically point to PDF documents. When the
        response is HTML (e.g. a Next.js page with embedded notice data),
        the adapter extracts text from the JSON payload or HTML content.
        When the response is a PDF or binary, fall back to subject-only
        extraction.
        """
        # BHEGTS detail URLs are often PDFs — check if this is HTML
        stripped = html.strip()
        if stripped.startswith("%PDF") or not stripped:
            # Binary/PDF content — can't parse, use subject only
            extraction = outage_extractor.extract_outage(
                subject=notice.get("subject", ""),
                detail_text="",
            )
            extraction["detail_text"] = ""
            return extraction

        # Try JSON parsing (Next.js SSR data)
        try:
            data = json.loads(html)
            text_parts = []
            if isinstance(data, dict):
                for key in ("subject", "body", "content", "text",
                            "noticeText", "description"):
                    val = data.get(key, "")
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

        # HTML parsing
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        body_text = ""

        # Try Next.js SSR: extract JSON from __NEXT_DATA__ script
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                nd = json.loads(next_data.string)
                page_props = nd.get("props", {}).get("pageProps", {})
                notice_data = page_props.get("notice", page_props.get("posting", {}))
                if isinstance(notice_data, dict):
                    text_parts = []
                    for key in ("subject", "body", "content", "text", "description"):
                        val = notice_data.get(key, "")
                        if val and isinstance(val, str):
                            text_parts.append(val)
                    if text_parts:
                        body_text = " ".join(text_parts)
            except (json.JSONDecodeError, ValueError):
                pass

        if not body_text:
            # Fallback: extract from HTML content
            content = soup.find("div", class_=re.compile(r"notice|content|detail|posting", re.IGNORECASE))
            if content:
                body_text = content.get_text(separator=" ", strip=True)
            else:
                body_text = soup.get_text(separator=" ", strip=True)

        body_text = " ".join(body_text.split())

        extraction = outage_extractor.extract_outage(
            subject=notice.get("subject", ""),
            detail_text=body_text,
        )
        extraction["detail_text"] = body_text[:5000]
        return extraction

    def _get_listing_sources(self) -> list[dict]:
        """Return one source per configured notice subcategory (Critical, Non-Critical)."""
        code = self.config["code"]
        base_url = self.config.get("base_url", "https://infopost.bhegts.com")
        subcategories = self.config.get(
            "notice_subcategories", ["Critical", "Non-Critical"]
        )

        sources = []
        for subcat in subcategories:
            # Fetch the page where JSON data is embedded
            subcat_slug = subcat.lower().replace(" ", "-")
            url = f"{base_url}/{code}/notices/notices-{subcat_slug}"
            sources.append(
                {
                    "url": url,
                    "subcategory": subcat,
                    "label": f"{self.pipeline_name} ({subcat})",
                }
            )
        return sources

    def _pull(self, url: str = "") -> str:
        """Fetch the page/API content. Try JSON API first, fall back to HTML page."""
        import requests
        from backend.scrapes.gas_ebbs.ebb_utils import DEFAULT_HEADERS

        target = url or self.listing_url

        headers = {**DEFAULT_HEADERS, "Accept": "application/json, text/html, */*"}
        response = requests.get(target, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        subcategory = kwargs.get("subcategory", "Critical")
        code = self.config["code"]
        base_url = self.config.get("base_url", "https://infopost.bhegts.com")

        notices = []

        # Attempt 1: Parse as JSON response (API endpoint)
        postings = self._try_parse_json(html)

        # Attempt 2: Extract JSON embedded in HTML page
        if postings is None:
            postings = self._try_extract_embedded_json(html)

        # Attempt 3: Fall back to HTML table parsing
        if postings is None:
            return self._parse_html_table(html, subcategory, code, base_url)

        # Process JSON postings
        for posting in postings:
            notice_id = str(posting.get("noticeId", posting.get("id", "")))
            if not notice_id:
                continue

            revision = str(posting.get("revision", "1"))

            # Build detail/document URL
            detail_url = (
                f"{base_url}/docs/{code}/postings/"
                f"{notice_id}/{revision}/{code}-{notice_id}.pdf"
            )

            # Check for explicit document URL in content array
            content = posting.get("content", [])
            if content and isinstance(content, list):
                for doc in content:
                    blob_url = doc.get("blobUrl", "")
                    if blob_url:
                        detail_url = blob_url
                        break

            notice = {
                "notice_type": posting.get(
                    "naesbNoticeTypeDescription",
                    posting.get("tspDefinedNoticeTypeDescription", ""),
                ),
                "notice_subtype": posting.get("subcategory", subcategory),
                "posted_datetime": posting.get("postedDate", ""),
                "effective_datetime": posting.get("effectiveDate", ""),
                "end_datetime": "",
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": posting.get("subject", ""),
                "response_datetime": posting.get("responseDate", ""),
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices

    def _try_parse_json(self, text: str) -> list[dict] | None:
        """Try to parse the response as a JSON array or object with postings."""
        try:
            data = json.loads(text)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # Look for a postings array in the response
                for key in ("postings", "data", "results", "notices", "items"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # If the dict itself looks like a single posting, wrap it
                if "noticeId" in data:
                    return [data]
            return None
        except (json.JSONDecodeError, ValueError):
            return None

    def _try_extract_embedded_json(self, html: str) -> list[dict] | None:
        """Try to extract JSON data embedded in HTML/script tags.

        BHEGTS uses Next.js SSR — notice data is in a <script> tag as
        JSON with structure: {"props":{"pageProps":{"notices":{"postings":[...]}}}}
        """
        # Try Next.js __NEXT_DATA__ or inline script with postings
        soup = BeautifulSoup(html, "html.parser")

        for script in soup.find_all("script"):
            script_text = script.string or ""
            if '"postings"' not in script_text:
                continue

            # Try parsing the entire script as JSON (Next.js __NEXT_DATA__)
            try:
                data = json.loads(script_text)
                # Navigate Next.js structure
                postings = (
                    data.get("props", {})
                    .get("pageProps", {})
                    .get("notices", {})
                    .get("postings", [])
                )
                if postings:
                    return postings
            except (json.JSONDecodeError, ValueError):
                pass

            # Fallback: extract postings array via regex
            match = re.search(r'"postings"\s*:\s*(\[.*?\])\s*[,}]', script_text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except (json.JSONDecodeError, ValueError):
                    continue

        # Also check raw HTML for postings patterns
        patterns = [
            r'"postings"\s*:\s*(\[.*?\])',
            r"postings\s*=\s*(\[.*?\]);",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except (json.JSONDecodeError, ValueError):
                    continue

        return None

    def _parse_html_table(
        self, html: str, subcategory: str, code: str, base_url: str
    ) -> list[dict]:
        """Fallback: parse notices from an HTML table."""
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

            # Try to find a numeric notice ID
            notice_id = None
            notice_id_idx = None
            for i, cell in enumerate(cells):
                candidate = clean_text(cell)
                if candidate and candidate.strip().isdigit():
                    notice_id = candidate
                    notice_id_idx = i
                    break

            if notice_id is None:
                continue

            # Extract fields relative to notice ID position
            notice_type_val = clean_text(cells[0]) if len(cells) > 0 else ""
            posted_dt = clean_text(cells[1]) if len(cells) > 1 else ""
            effective_dt = clean_text(cells[2]) if len(cells) > 2 else ""

            # Subject is typically after the notice ID
            subj_idx = notice_id_idx + 1 if notice_id_idx + 1 < len(cells) else 0
            subject_text = clean_text(cells[subj_idx])

            detail_url = (
                f"{base_url}/docs/{code}/postings/"
                f"{notice_id}/1/{code}-{notice_id}.pdf"
            )

            notice = {
                "notice_type": notice_type_val,
                "notice_subtype": subcategory,
                "posted_datetime": posted_dt,
                "effective_datetime": effective_dt,
                "end_datetime": "",
                "notice_identifier": notice_id,
                "notice_status": "",
                "subject": subject_text,
                "response_datetime": "",
                "detail_url": detail_url,
            }
            notices.append(notice)

        return notices
