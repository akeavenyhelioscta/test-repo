"""
Standalone source-family adapter.

Generic fallback adapter for pipelines with unique EBBs that don't fit
into other source families. Uses generic HTML table parsing.

Each pipeline has its own listing_url configured in standalone.yaml.
The adapter attempts to find and parse the largest HTML table on the page.
"""

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs_v2.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs_v2.ebb_utils import clean_text, extract_numeric_id


@register_adapter("standalone")
class StandaloneAdapter(EBBScraper):
    """Generic adapter for standalone pipeline EBBs.

    Attempts to parse notice data from the largest HTML table on the page.
    Works as a best-effort fallback for pipelines without dedicated adapters.

    Looks for rows with at least 5 cells and tries to identify:
    - A cell with a numeric value (notice ID)
    - Surrounding cells with date/time and text content
    """

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
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

            # Scan cells for a numeric notice ID
            notice_id = None
            notice_id_idx = None

            for i, cell in enumerate(cells):
                candidate = clean_text(cell)
                if candidate and candidate.strip().isdigit() and len(candidate.strip()) >= 3:
                    notice_id = candidate
                    notice_id_idx = i
                    break

            if notice_id is None:
                continue

            # Heuristic field extraction based on ID position
            # Common layouts put ID at position 4 (after type, posted, effective, end)
            if notice_id_idx >= 4:
                notice_type_val = clean_text(cells[0])
                posted_dt = clean_text(cells[1])
                effective_dt = clean_text(cells[2])
                end_dt = clean_text(cells[3])
                subj_idx = notice_id_idx + 1
                subject_text = clean_text(cells[subj_idx]) if subj_idx < len(cells) else ""
                resp_idx = notice_id_idx + 2
                response_dt = clean_text(cells[resp_idx]) if resp_idx < len(cells) else ""
            elif notice_id_idx >= 1:
                notice_type_val = clean_text(cells[0])
                posted_dt = ""
                effective_dt = ""
                end_dt = ""
                subject_text = ""
                response_dt = ""
                # Try to extract dates from cells before and subject from cells after
                if notice_id_idx >= 2:
                    posted_dt = clean_text(cells[1])
                if notice_id_idx >= 3:
                    effective_dt = clean_text(cells[2])
                if notice_id_idx >= 4:
                    end_dt = clean_text(cells[3])
                subj_idx = notice_id_idx + 1
                if subj_idx < len(cells):
                    subject_text = clean_text(cells[subj_idx])
                resp_idx = notice_id_idx + 2
                if resp_idx < len(cells):
                    response_dt = clean_text(cells[resp_idx])
            else:
                # ID is the first cell — unusual layout
                notice_type_val = ""
                posted_dt = clean_text(cells[1]) if len(cells) > 1 else ""
                effective_dt = clean_text(cells[2]) if len(cells) > 2 else ""
                end_dt = clean_text(cells[3]) if len(cells) > 3 else ""
                subject_text = clean_text(cells[4]) if len(cells) > 4 else ""
                response_dt = ""

            # Try to extract detail URL from any link in the subject cell
            detail_url = ""
            if notice_id_idx + 1 < len(cells):
                subject_cell = cells[notice_id_idx + 1]
                subject_link = subject_cell.find("a")
                if subject_link and subject_link.get("href"):
                    href = subject_link["href"]
                    if href.startswith("http"):
                        detail_url = href
                    elif href.startswith("/"):
                        # Best-effort: use the listing URL's host
                        from urllib.parse import urlparse
                        parsed = urlparse(self.listing_url)
                        detail_url = f"{parsed.scheme}://{parsed.netloc}{href}"

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
