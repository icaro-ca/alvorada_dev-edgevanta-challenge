import re
import pdfplumber

from extractors.base_extractor import BasePDFExtractor


class ItemCReportExtractor(BasePDFExtractor):
    """
    Extracts all contracts from a NC DOT Item C (Highway Letting) PDF.

    Because each page is an independent contract record, ``_extract``
    returns a **list of dicts** (one per page) rather than a single dict.
    The base-class envelope wraps this list under the ``"data"`` key.

    Returned ``data`` item keys
    ----------------------------
    page_num, total_pages, letting_date, district, owner, state,
    contract_id, wbs_element, project_number, county, tip_number,
    proposal_length, type_of_work, location, rpn, bidder_count,
    dbe_goal, estimate, date_available, final_completion,
    inter_completion, inter_completion_description, bidders,
    estimate_total, letting_total, letting_total_pct_diff
    """

    extractor_name = "ItemCReport"

    _MONTHS = (
        r'(?:JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|'
        r'SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)'
    )
    _TIP_RE = re.compile(r'^[A-Z]-\d+', re.IGNORECASE)

    def _find(self, pattern, text, flags=re.IGNORECASE):
        m = re.search(pattern, text, flags)
        return m.group(1).strip() if m else None

    def _fix_wrapped_city_lines(self, lines: list) -> list:
        """
        Rejoin city lines where the two-letter state code wrapped to the next line.
        e.g.  "CRUMB...GREENVILLE, 718,768.60 +16.5" + "NC"
           →  "CRUMB...GREENVILLE, NC 718,768.60 +16.5"
        """
        
        fixed = []
        i = 0
        while i < len(lines):
            line    = lines[i]
            next_ln = lines[i + 1].strip() if i + 1 < len(lines) else ''
            if (
                re.match(r'^[A-Z]{2}$', next_ln)
                and re.search(r'[\d,]+\.\d{2}\s+[+-][\d.]+\s*$', line)
            ):
                m = re.search(r'(\s+[\d,]+\.\d{2}\s+[+-][\d.]+\s*)$', line)
                if m:
                    fixed.append(line[:m.start()] + ' ' + next_ln + m.group(1))
                    i += 2
                    continue
            fixed.append(line)
            i += 1
        return fixed

    def _extract_page(self, page_text: str, page_num: int, total_pages: int) -> dict:
        """Extract all contract fields from a single page's text."""
        lines  = page_text.splitlines()
        result = {'page_num': page_num, 'total_pages': total_pages}

        # ── Letting date & Division ──────────────────────────────────────────
        date_m = re.search(rf'({self._MONTHS}\s+\d{{1,2}},\s+\d{{4}})', page_text, re.IGNORECASE)
        result['letting_date'] = date_m.group(1).title() if date_m else None

        div_m = re.search(r'^(DIVISION\s+\d+)$', page_text, re.MULTILINE)
        result['district'] = div_m.group(1).title() if div_m else None

        # ── Owner + State ────────────────────────────────────────────────────
        so_m = re.search(r'^HIGHWAY LETTING[^\n]*\n(.+?)\s+ITEM C\b', page_text, re.MULTILINE)
        if so_m:
            owner_raw       = so_m.group(1).strip()
            result['owner'] = owner_raw.title()
            state_m         = re.match(r'^(.+?)\s+(?:DEPARTMENT|DEPT|DOT)\b', owner_raw, re.IGNORECASE)
            result['state'] = state_m.group(1).title() if state_m else None
        else:
            result['owner'] = None
            result['state'] = None

        # ── Contract block ───────────────────────────────────────────────────
        div_idx = next(
            (i for i, l in enumerate(lines) if re.match(r'^DIVISION\s+\d+$', l.strip())), None,
        )
        if div_idx is not None:
            cb = [l.strip() for l in lines[div_idx + 1:] if l.strip()]
            result['contract_id'] = cb[0] if cb else None

            pl_idx  = next((j for j, l in enumerate(cb) if l.startswith('PROPOSAL LENGTH')), None)
            between = cb[1:pl_idx] if pl_idx else cb[1:]

            if between and self._TIP_RE.match(between[-1]):
                result['tip_number'] = between[-1]
                between = between[:-1]
            else:
                result['tip_number'] = None

            result['county']         = between[-1]  if len(between) >= 1 else None
            result['project_number'] = between[-2]  if len(between) >= 2 else None
            wbs_lines                = between[:-2] if len(between) >= 2 else []
            result['wbs_element']    = (
                re.sub(r',\s*,', ',', ', '.join(wbs_lines)).strip(', ') if wbs_lines else None
            )
        else:
            result['contract_id']    = None
            result['wbs_element']    = None
            result['project_number'] = None
            result['county']         = None
            result['tip_number']     = None

        result['proposal_length'] = self._find(r'^PROPOSAL LENGTH\s+(.+?)$', page_text, re.MULTILINE)
        result['type_of_work']    = self._find(r'^TYPE OF WORK\s+(.+?)$',    page_text, re.MULTILINE)
        result['location']        = self._find(r'^LOCATION\s+(.+?)$',        page_text, re.MULTILINE)

        result['rpn']              = self._find(r'\bRPN\s+(\d+)', page_text)
        result['bidder_count']     = self._find(r'(\d+)\s+BIDDER\(S\)', page_text)
        result['dbe_goal']         = self._find(r'DBE GOAL\s+([\d.]+\s*%)', page_text)
        result['estimate']         = self._find(r'\bESTIMATE\s+([\d,]+\.?\d*)', page_text)
        result['date_available']   = self._find(r'^DATE AVAILABLE\s+(.+?)$', page_text, re.MULTILINE)
        result['final_completion'] = self._find(r'^FINAL COMPLETION\s+(.+?)$', page_text, re.MULTILINE)

        inter_m = re.search(
            r'^INTER COMPLETION\s+(\w+\s+\d+\s+\d+)\s+(.*?)(?=\nFINAL COMPLETION)',
            page_text, re.MULTILINE | re.DOTALL,
        )
        if inter_m:
            result['inter_completion']             = inter_m.group(1).strip()
            result['inter_completion_description'] = re.sub(r'\s+', ' ', inter_m.group(2)).strip()
        else:
            result['inter_completion']             = None
            result['inter_completion_description'] = None

        totals_idx = next(
            (i for i, l in enumerate(lines) if '$ TOTALS' in l and '% DIFF' in l), None,
        )
        bidders = []
        if totals_idx is not None:
            raw = []
            for l in lines[totals_idx + 1:]:
                s = l.strip()
                if re.match(r'^(ESTIMATE TOTAL|LETTING TOTAL|HIGHWAY LETTING)', s, re.I):
                    break
                raw.append(l)

            raw = self._fix_wrapped_city_lines(raw)

            for line in raw:
                line = line.strip()
                if not line:
                    continue
                m = re.match(r'^(.+?)\s+([\d,]+\.\d{2})\s+([+-][\d.]+)\s*$', line)
                if not m:
                    continue

                name_city = m.group(1).strip()
                total     = m.group(2)
                pct_diff  = m.group(3)

                _nc = re.match(
                    r'^(.*\b(?:LLC|INC\.?|CORP(?:ORATION)?|COMPANY|TRUCKING|TOWING|'
                    r'SHIPYARD|CONSTRUCTION|CO\.?|SERVICES|ENTERPRISES|'
                    r'CONTRACTORS?|CONTRACTING|ASSOCIATES)\.?)'
                    r'\s+([A-Za-z][A-Za-z -]*?),\s*([A-Z]{2})$',
                    name_city, re.IGNORECASE,
                )
                if _nc:
                    name  = _nc.group(1).strip()
                    city  = _nc.group(2).strip()
                    state = _nc.group(3)
                else:
                    fb    = re.match(r'^(.*),\s*([A-Z]{2})$', name_city)
                    name  = fb.group(1).strip() if fb else name_city
                    city  = ''
                    state = fb.group(2) if fb else ''

                bidders.append({
                    'name': name, 'city': city, 'state': state,
                    'total': total, 'pct_diff': pct_diff,
                })

        result['bidders'] = bidders

        result['estimate_total'] = self._find(
            r'^ESTIMATE TOTAL\s+([\d,]+\.?\d*)', page_text, re.MULTILINE,
        )
        lt_m = re.search(
            r'^LETTING TOTAL\s+([\d,]+\.?\d*)\s*([+-][\d.]+)?', page_text, re.MULTILINE,
        )
        result['letting_total']          = lt_m.group(1) if lt_m else None
        result['letting_total_pct_diff'] = lt_m.group(2) if lt_m else None

        return result

    # Core extraction
    def _extract(self, pdf_path: str) -> list[dict]:
        """
        Extract all contract records from the PDF, one dict per page.
        """

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            pages_text  = [p.extract_text() or '' for p in pdf.pages]

        return [
            self._extract_page(text, page_num=i + 1, total_pages=total_pages)
            for i, text in enumerate(pages_text)
        ]