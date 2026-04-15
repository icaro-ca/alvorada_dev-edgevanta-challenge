import re
import pdfplumber

from extractors.base_extractor import BasePDFExtractor


class AwardLetterExtractor(BasePDFExtractor):
    """
    Extracts structured data from a NC DOT Notification of Award PDF.

    Returned ``data`` keys
    ----------------------
    contract_id, tip_number, project_number, wbs_element, county,
    proposal_description, letter_date, vendor, letting_date, bid_value,
    state, owner, signed_by, signed_title
    """

    extractor_name = "AwardLetter"

    def _extract(self, pdf_path: str) -> dict:
        full_text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

        def find(pattern, text=full_text, flags=re.IGNORECASE):
            m = re.search(pattern, text, flags)
            return m.group(1).strip() if m else None

        data: dict = {}

        data["contract_id"]   = find(r"Contract No\.\s*(.+)")
        data["tip_number"]    = find(r"TIP No\.:\s*(.+)")
        data["project_number"] = find(r"Federal Aid No\.:\s*(.+)")
        data["wbs_element"]   = find(r"WBS Element:\s*(.+)")

        data["county"] = find(r"County:\s*(.+)") or find(r"Counties:\s*(.+)")

        desc_match = re.search(
            r"Description:\s*(.+?)(?=\nDear|\nMailing)", full_text, re.DOTALL
        )
        data["proposal_description"] = (
            " ".join(desc_match.group(1).split()) if desc_match else None
        )

        data["letter_date"] = find(
            r"NOTIFICATION OF AWARD\s*\n([A-Z][a-z]+ \d{1,2}, \d{4})"
        )
        data["vendor"] = find(r"inform you that\s+(.+?)\s+has been awarded")
        data["letting_date"] = find(
            r"bid submitted on\s+([A-Z][a-z]+ \d{1,2}, \d{4})"
        )
        data["bid_value"] = find(
            r"bid submitted on\s+[A-Z][a-z]+\s+\d{1,2},\s+\d{4}\s+in the\s+amount of\s+(\$[\d,]+\.\d{2})"
        )

        state_match = re.search(r"STATE OF ([A-Z ]+)", full_text)
        owner_match = re.search(r"STATE OF [A-Z ]+\s*\n\s*([A-Z][A-Z ]+)", full_text)

        state      = state_match.group(1).strip() if state_match else None
        owner_line = owner_match.group(1).strip() if owner_match else None

        data["state"] = state.title() if state else None
        data["owner"] = (
            f"{state.title()} {owner_line.title()}" if state and owner_line else None
        )

        data["signed_by"]    = find(r"Sincerely,\s*\n+([A-Z][^\n]+)")
        data["signed_title"] = find(r"Sincerely,\s*\n+[A-Z][^\n]+\n([A-Z][^\n]+)")

        return data