import base64
import json
import os
import re
from io import BytesIO
from pathlib import Path

import pypdfium2 as pdfium
import anthropic
from PIL import Image

from extractors.base_extractor import BasePDFExtractor

_MODEL = "claude-sonnet-4-20250514"
_DPI   = 200

_SYSTEM = """You are a precise data extraction assistant.
You will be shown scanned images of NC DOT "Contract Bids as Read" forms.
Extract ALL fields exactly as written. Return ONLY valid JSON, no markdown fences."""

_PROMPT = """Extract all data from this NC DOT Contract Bids as Read form page.

Return a JSON object with exactly these keys:
{
  "revised_date": string or null,
  "state": string or null,
  "owner": string or null,
  "letting_date": string or null,
  "bid_opening_time": string or null,
  "contract_id": string or null,
  "description": string or null,
  "counties": string or null,
  "total_bids_received": number or null,
  "engineers_estimate": string or null,
  "corrected_engineers_estimate": string or null,
  "bids": [
    {
      "contractor": string,
      "bid_amount": string,
      "corrected_amount": string or null
    }
  ]
}

Rules:
- Transcribe contractor names EXACTLY as handwritten
- Include $ in all amounts (e.g. "$724,140.00")
- If a bid has a handwritten correction marked with * or MSW,
  put the corrected value in corrected_amount
- total_bids_received must be an integer
- For contract_id, preserve the full value as written (e.g. "12107176 / MA00004")
- Return ONLY the JSON object, no explanation"""


# ---------------------------------------------------------------------------
# Extractor class
# ---------------------------------------------------------------------------

class BidsAsReadExtractor(BasePDFExtractor):
    """
    Extracts structured data from NC DOT "Contract Bids as Read" PDFs
    using Claude Vision API.

    Because these are scanned handwritten forms, pdfplumber cannot reliably
    extract text — each page is rasterised to JPEG and sent to the Vision API.

    Returned ``data`` keys (list of dicts, one per page)
    -----------------------------------------------------
    page, revised_date, state_owner, letting_date, bid_opening_time,
    contract_id, description, counties, total_bids_received,
    engineers_estimate, corrected_engineers_estimate, bids
    """

    extractor_name = "BidsAsRead"

    def __init__(self, dpi: int = _DPI):
        self._dpi = dpi

    def _pdf_to_images(self, pdf_path: str) -> list[Image.Image]:
        """Rasterise every page of the PDF to a PIL image."""
        scale = self._dpi / 72
        doc   = pdfium.PdfDocument(str(Path(pdf_path).resolve()))
        return [page.render(scale=scale, rotation=0).to_pil() for page in doc]

    def _img_to_b64(self, img: Image.Image) -> str:
        """Encode a PIL image as base64 JPEG."""
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=92)
        return base64.standard_b64encode(buf.getvalue()).decode()

    def _extract_page(self, client, img: Image.Image, page_num: int) -> dict:
        """Send one page image to the Vision API and parse the response."""

        response = client.messages.create(
            model=_MODEL,
            max_tokens=1000,
            system=_SYSTEM,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type":       "base64",
                            "media_type": "image/jpeg",
                            "data":       self._img_to_b64(img),
                        }
                    },
                    {
                        "type": "text",
                        "text": _PROMPT,
                    }
                ]
            }]
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$",          "", raw)

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            data = {"_parse_error": str(e), "_raw": raw}

        data["page"] = page_num
        return data

    def _extract(self, pdf_path: str) -> list[dict]:

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "ANTHROPIC_API_KEY environment variable is not set."
            )

        client = anthropic.Anthropic(api_key=api_key)

        images  = self._pdf_to_images(pdf_path)
        results = []

        for i, img in enumerate(images, 1):
            record = self._extract_page(client, img, page_num=i)
            results.append(record)

        return results