"""
Tallgrass source-family adapter.

Handles all pipelines hosted on pipeline.tallgrassenergylp.com (4 pipelines).
The Tallgrass EBB may require JavaScript rendering; this adapter attempts
HTML table parsing and will need enhancement with Selenium for full support.

Pipelines: Rockies Express (REX), Ruby, Tallgrass Interstate, Trailblazer

EBB: http://pipeline.tallgrassenergylp.com/
"""

from bs4 import BeautifulSoup

from backend.scrapes.gas_ebbs.base_scraper import EBBScraper, register_adapter
from backend.scrapes.gas_ebbs.ebb_utils import clean_text, extract_numeric_id


BASE_URL = "http://pipeline.tallgrassenergylp.com"


@register_adapter("tallgrass")
class TallgrassAdapter(EBBScraper):
    """Adapter for Tallgrass pipeline EBB pages.

    All Tallgrass pipelines are hosted on the same domain.
    The site may use JavaScript rendering; this adapter provides
    HTML table parsing as a baseline. May need Selenium for full support.

    Expected table columns (when data is available):
        0: Notice Type
        1: Posted Date/Time
        2: Effective Date/Time
        3: End Date/Time
        4: Notice ID (numeric)
        5: Subject
        6: Response Date/Time
    """

    def _get_listing_sources(self) -> list[dict]:
        """Return listing URLs for the pipeline."""
        path = self.config.get("path", "")
        base = self.config.get("base_url", BASE_URL)

        if path:
            url = f"{base}/{path}"
        else:
            url = base

        return [
            {
                "url": url,
                "label": f"{self.pipeline_name}",
            }
        ]

    def _parse_listing(self, html: str, **kwargs) -> list[dict]:
        base = self.config.get("base_url", BASE_URL)

        soup = BeautifulSoup(html, "html.parser")

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

            # Scan for a numeric notice ID
            notice_id = None
            notice_id_idx = None

            if len(cells) > 4:
                candidate = clean_text(cells[4])
                if candidate and extract_numeric_id(candidate) and candidate.strip().isdigit():
                    notice_id = candidate
                    notice_id_idx = 4

            if notice_id is None:
                for i, cell in enumerate(cells):
                    candidate = clean_text(cell)
                    if candidate and candidate.strip().isdigit():
                        notice_id = candidate
                        notice_id_idx = i
                        break

            if notice_id is None:
                continue

            if notice_id_idx == 4 and len(cells) >= 6:
                notice_type_val = clean_text(cells[0])
                posted_dt = clean_text(cells[1])
                effective_dt = clean_text(cells[2])
                end_dt = clean_text(cells[3])
                subject_cell = cells[5]
                response_dt = clean_text(cells[6]) if len(cells) >= 7 else ""
            else:
                notice_type_val = clean_text(cells[0])
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
                    detail_url = f"{base}/{href}"

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
