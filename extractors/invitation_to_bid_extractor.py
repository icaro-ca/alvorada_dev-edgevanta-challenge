import re
import pdfplumber

from extractors.base_extractor import BasePDFExtractor


class InvitationToBidExtractor(BasePDFExtractor):
    """
    Auto-detects ITB variant (SBE or Standard) and extracts structured fields.

    Returned ``data`` keys (Standard variant)
    ------------------------------------------
    variant, state, owner, letter_date, district, contract_id,
    proposal_description, wbs_element, county, date_available,
    completion_date, mbe_goal, wbe_goal, combined_goal, letting_date,
    contact_name, contact_phone, signed_by, signer_title

    Returned ``data`` keys (SBE variant)
    --------------------------------------
    variant, state, owner, letter_date, district, contract_id,
    proposal_description, county, letting_date, date_available,
    completion_date, contact_name, contact_phone, sbe_income_limit
    """

    extractor_name = "InvitationToBid"

    _MONTHS = (
        r'(?:January|February|March|April|May|June|July|August|'
        r'September|October|November|December)'
    )

    def _find(self, pattern, text, flags=re.IGNORECASE):
        """Return the first capture group of *pattern*, or None."""
        m = re.search(pattern, text, flags)
        return m.group(1).strip() if m else None

    def _find_date(self, pattern, text, flags=re.IGNORECASE):
        """Return first match as title-cased string (e.g. 'January 29, 2025')."""
        m = re.search(pattern, text, flags)
        return m.group(1).strip().title() if m else None

    def _detect_variant(self, all_text: str) -> str:
        """
        Return ``'SBE'`` if the document contains an SBE PROGRAM section, else ``'Standard'``.
        """
        if re.search(r'SBE PROGRAM', all_text, re.IGNORECASE):
            return 'SBE'
        return 'Standard'

    def _extract_owner_state(self, all_text: str) -> tuple[str | None, str | None]:
        """Extract state and full owner name from the letterhead block."""
        state_match = re.search(r"STATE OF ([A-Z ]+)", all_text)
        owner_match = re.search(r"STATE OF [A-Z ]+\s*\n\s*([A-Z][A-Z ]+)", all_text)
        state = state_match.group(1).strip() if state_match else None
        owner = owner_match.group(1).strip() if owner_match else None
        return (
            state.title() if state else None,
            f"{state.title()} {owner.title()}" if state and owner else None,
        )

    def _extract_county(self, proposal_description: str | None, pattern: str) -> str | None:
        """Extract county name(s) from the description, stripping the County/Counties suffix."""
        county_m = re.search(pattern, proposal_description or '', re.IGNORECASE)
        if not county_m:
            return None
        county = re.sub(r',?\s*Counti?(?:es|y)?\s*$', '', county_m.group(1), flags=re.IGNORECASE)
        return county.strip()

    def _extract_sbe(self, pages_text: list[str]) -> dict:
        """Extract fields from an SBE ITB. Contract ID is in a 'QUOTATION FOR' sentence."""
        p1       = pages_text[0]
        all_text = '\n'.join(pages_text)
        flat     = re.sub(r'\s+', ' ', all_text)

        state, owner = self._extract_owner_state(all_text)
        result = {'variant': 'SBE', 'state': state, 'owner': owner}

        result['letter_date'] = self._find_date(
            rf'^({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})$', p1,
            re.MULTILINE | re.IGNORECASE,
        )
        result['district']    = self._find(r'(Division\s+(?:One|Two|Three|\d+))', p1)
        result['contract_id'] = self._find(r'QUOTATION FOR\s+(DA\d+)', all_text)
        if not result['contract_id']:
            result['contract_id'] = self._find(r'^(DA\d+)\s*\nPage\s+2', all_text, re.MULTILINE)

        p1_clean = re.sub(r'-\n', '-', p1)
        desc_m   = re.search(
            r'requesting bids for the (.+)\. Except\s+as\s+otherwise noted',
            p1_clean, re.DOTALL | re.IGNORECASE,
        )
        result['proposal_description'] = desc_m.group(1).strip() if desc_m else None

        result['county'] = self._extract_county(
            result['proposal_description'],
            r',?\s*in\s+([A-Za-z][A-Za-z\s,&]+?(?:Counties|County)'
            r'(?:\s*,\s*[A-Za-z]+\s+(?:Counties|County))*)(?:[.\s]|$)',
        )

        result['letting_date'] = self._find_date(
            rf'(?:on\s+|ON\s+(?:WEDNESDAY,\s+)?)({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            all_text,
        )
        result['date_available'] = self._find_date(
            rf'date of availability for this contract is\s+({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            all_text,
        )
        result['completion_date'] = self._find_date(
            rf'completion date for this contract is\s+({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            all_text,
        )
        result['contact_name']  = self._find(
            r'please contact\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+at\s+\(', flat,
        )
        result['contact_phone'] = self._find(
            r'please contact\s+[A-Za-z\s.]+at\s+(\(\d{3}\)\s*\d{3}-\d{4})', flat,
        )
        result['sbe_income_limit'] = self._find(
            r'annual gross\s+income of\s+(\$[\d,]+(?:\.\d+)?(?:\s+or less)?)', all_text,
        )
        return result

    def _extract_standard(self, pages_text: list[str]) -> dict:
        """Extract fields from a Standard ITB. Contract ID precedes the em-dash in the header."""
        p1       = pages_text[0]
        all_text = '\n'.join(pages_text)

        state, owner = self._extract_owner_state(all_text)
        result = {'variant': 'Standard', 'state': state, 'owner': owner}

        result['letter_date'] = self._find_date(
            rf'^({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})$', p1,
            re.MULTILINE | re.IGNORECASE,
        )
        result['district']    = self._find(
            r'project in\s+(Division\s+(?:One|Two|Three|\d+))', p1, re.IGNORECASE,
        )
        result['contract_id'] = self._find(r'\b(DA\d+)\s*[-–]', p1)

        desc_m = re.search(r'DA\d+\s*[-–]\s*(.+?)(?=\nWBS Element)', p1, re.DOTALL)
        result['proposal_description'] = (
            re.sub(r'\s+', ' ', desc_m.group(1)).strip() if desc_m else None
        )
        result['wbs_element'] = self._find(r'WBS Element:\s*(.+?)(?:\n|$)', p1)

        result['county'] = self._extract_county(
            result['proposal_description'],
            r'in\s+((?:[A-Z][a-z]+(?:,?\s*(?:&\s*)?)?)+(?:Counties|County))',
        )

        result['date_available'] = self._find_date(
            rf'Date of Availability for this Contract is\s+({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            p1,
        )
        result['completion_date'] = self._find_date(
            rf'Completion Date for this Contract is\s+({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            p1,
        )
        result['mbe_goal']      = self._find(r'Minority Business Enterprise Goal\s*=\s*([\d.]+%)', p1)
        result['wbe_goal']      = self._find(r'Women Business Enterprise Goal\s*=\s*([\d.]+%|0%)', p1)
        result['combined_goal'] = self._find(r'Combined MBE/WBE Goal\s*=\s*([\d.]+%)', p1)
        result['letting_date']  = self._find_date(
            rf'Bid Opening will be at 2:00 pm on (?:Wednesday\s+)?({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})',
            p1,
        )
        result['contact_name']  = self._find(
            r'please contact\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s+at\s+\(', all_text,
        )
        result['contact_phone'] = self._find(
            r'please contact\s+[A-Za-z\s.]+at\s+(\(\d{3}\)\s*\d{3}-\d{4})', all_text,
        )

        signer_m = re.search(
            r'Sincerely,\s*\n(?:.*\n){0,3}?([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)\s*\n'
            r'(Division Contract Engineer)',
            all_text,
        )

        result['signed_by']    = signer_m.group(1).strip() if signer_m else None
        result['signer_title'] = signer_m.group(2).strip() if signer_m else None

        return result

    # Core extraction
    def _extract(self, pdf_path: str) -> dict:
        """
        Detect the ITB variant and delegate to the appropriate extractor.
 
        Reads all pages, detects the variant from the full text, then
        calls ``_extract_sbe`` or ``_extract_standard`` accordingly.
        """
        
        with pdfplumber.open(pdf_path) as pdf:
            pages_text = [p.extract_text() or '' for p in pdf.pages]

        all_text = '\n'.join(pages_text)
        variant  = self._detect_variant(all_text)

        if variant == 'SBE':
            return self._extract_sbe(pages_text)
        return self._extract_standard(pages_text)