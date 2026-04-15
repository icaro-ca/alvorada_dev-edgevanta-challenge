import re
import pdfplumber

from extractors.base_extractor import BasePDFExtractor


class BidTabsExtractor(BasePDFExtractor):
    """
    Extracts structured data from NC DOT Bid Tabs PDFs.

    Returned ``data`` keys
    ----------------------
    report_timestamp, letting_time, county, call_number, total_pages,
    letting_date, contract_id, wbs_elements, tip_number, project_number,
    total_miles, description, location, signed_date,
    bidders, line_items, section_totals, contract_totals
    """

    extractor_name = "BidTabs"

    # Constants 
    _UNITS = {
        'LF', 'SY', 'TON', 'GAL', 'EA', 'CY', 'SF', 'SMI', 'LB', 'ACR',
        'LS', 'CWT', 'FT', 'MI', 'MSF', 'MGAL', 'HR', 'DAY', 'MO', 'MHR',
        'MSY', 'CF', 'CS',
    }
    _PRICE_RE          = re.compile(r'^\d[\d,]*\.\d+$')
    _QTY_RE            = re.compile(r'^\d[\d,]*\.?\d*$')
    _TRAILING_PAREN_RE = re.compile(r'\s*\((?:[^()]*|\([^()]*\))*\)\s*$')


    # Private helpers
    def _find(self, pattern, text, flags=re.IGNORECASE):
        m = re.search(pattern, text, flags)
        return m.group(1).strip() if m else None

    def _strip_trailing_parens(self, line: str) -> str:
        """
        Repeatedly remove trailing parenthetical groups from *line*.
 
        Station references like "(15 + 20.00 -L-)" and other annotations
        appended after the last price token are stripped so they do not
        interfere with right-to-left price parsing.
        """
        prev = None
        while prev != line:
            prev = line
            line = self._TRAILING_PAREN_RE.sub('', line).rstrip()
        return line

    def _truncate_at_last_price(self, line: str) -> str:
        """
        Strip trailing parentheticals then cut *line* at its last price token.
 
        After stripping parentheticals, everything after the rightmost
        decimal-number token is removed. This handles description text
        that continues past the price columns (e.g. "JOINT REPAIR",
        "HOT SPRAY THERMOPLASTIC", continuation lines).
        """
        line   = self._strip_trailing_parens(line)
        tokens = line.split()
        last_price_idx = -1
        for i, t in enumerate(tokens):
            if self._PRICE_RE.match(t):
                last_price_idx = i
        if last_price_idx >= 0:
            return ' '.join(tokens[:last_price_idx + 1])
        return line

    def _is_item_start(self, line: str) -> bool:
        """
        Return True if *line* begins a new bid item.
 
        Item start pattern: four-digit line number followed by a
        ten-digit item number and a letter suffix, e.g.
        ``0001 0000100000-N``.
        """
        return bool(re.match(r'^\d{4}\s+\d{10}-[A-Z]', line.strip()))

    def _extract_item_lines(self, page_text: str) -> list:
        """
        Return fully merged, trailing-cleaned item lines from one page.
        Merges continuation lines and truncates each merged line at its
        last price token.
        """
        lines      = page_text.splitlines()
        item_start = next((i for i, l in enumerate(lines) if self._is_item_start(l)), None)
        if item_start is None:
            return []
        item_end = next(
            (i for i, l in enumerate(lines)
             if re.match(r'^CONTRACT TOTAL', l.strip(), re.I) and i > item_start),
            len(lines),
        )
        merged = []
        for line in lines[item_start:item_end]:
            s = line.strip()
            if not s:
                continue
            if self._is_item_start(s):
                merged.append(self._truncate_at_last_price(s))
            elif merged and not re.match(
                r'^(CONTRACT TOTAL|ROADWAY ITEMS|STRUCTURE ITEMS|BIDDERS IN ORDER|TIP NO|FED AID|'
                r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s)', s, re.I,
            ):
                last_tok = merged[-1].split()[-1] if merged[-1].split() else ''
                if not self._PRICE_RE.match(last_tok):
                    candidate  = merged[-1] + ' ' + s
                    merged[-1] = self._truncate_at_last_price(candidate)
        return merged

    def _find_city_line(self, page_text: str) -> str | None:
        """
        Find the bidder header line that contains city/state pairs.
 
        This line (e.g. "RALEIGH, NC  ROCKY MOUNT, NC") is used to
        count the number of bidder columns on the current page. Lines
        that match road/county patterns or item-start patterns are
        excluded to avoid false positives.
        """
        for line in page_text.splitlines():
            if not re.search(r',\s+[A-Z]{2}(?![A-Z0-9\-])', line, re.I):
                continue
            if re.search(r'COUNTY|ROUTE|ROAD|STREET|HIGHWAY|VARIOUS|TIP NO|FED AID', line, re.I):
                continue
            if self._is_item_start(line):
                continue
            return line
        return None

    def _parse_item_line(self, line: str) -> dict | None:
        """Parse a single merged item line into structured fields.
 
        Tokens are consumed right-to-left:
          1. Collect trailing decimal numbers as prices.
          2. Identify the unit token (if it matches a known abbreviation).
          3. Identify the quantity token (last remaining numeric token),
             or set to "Lump Sum" if the ``__LUMP__`` sentinel is present.
          4. Remaining tokens form the item description.
 
        Returns None if the line does not match the expected item pattern.
        """
        m = re.match(r'^(\d{4})\s+(\d{10}-[A-Z])\s+([A-Z0-9]+)\s+(.*)', line.strip())
        if not m:
            return None
        item_line, item_num, cat, rest = m.groups()
        tokens = rest.split()

        is_lump = False
        for j, t in enumerate(tokens):
            if t == 'Lump' and j + 1 < len(tokens) and tokens[j + 1] == 'Sum':
                tokens  = tokens[:j] + ['__LUMP__'] + tokens[j + 2:]
                is_lump = True
                break

        prices = []
        i = len(tokens) - 1
        while i >= 0 and self._PRICE_RE.match(tokens[i]):
            prices.insert(0, tokens[i])
            i -= 1

        if is_lump and not prices:
            while i >= 0 and self._QTY_RE.match(tokens[i]) and tokens[i] != '__LUMP__':
                prices.insert(0, tokens[i])
                i -= 1

        remaining = tokens[:i + 1]

        unit = None
        if remaining and remaining[-1].upper() in self._UNITS:
            unit      = remaining[-1].upper()
            remaining = remaining[:-1]

        qty = None
        if is_lump:
            qty       = 'Lump Sum'
            remaining = [t for t in remaining if t != '__LUMP__']
        elif remaining and self._QTY_RE.match(remaining[-1]):
            qty       = remaining[-1]
            remaining = remaining[:-1]

        return {
            'item_line':   item_line,
            'item_number': item_num,
            'category':    cat,
            'description': ' '.join(remaining),
            'quantity':    qty,
            'unit':        unit,
            'is_lump':     is_lump,
            '_prices':     prices,
        }

    # Core extraction
    def _extract(self, pdf_path: str) -> dict:
        """
        Parse the Bid Tabs PDF and return all structured fields.
        """

        with pdfplumber.open(pdf_path) as pdf:
            pages_text = [p.extract_text() or '' for p in pdf.pages]

        n_pages   = len(pages_text)
        page1     = pages_text[0]
        last_page = pages_text[-1]
        all_text  = '\n\n'.join(pages_text)
        result: dict = {}

        lines1 = page1.splitlines()

        result['report_timestamp'] = self._find(
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
            r'\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s+[AP]M)', page1,
        )
        times = re.findall(r'\d{1,2}:\d{2}\s*[AP]M', lines1[0]) if lines1 else []
        result['letting_time']  = times[1] if len(times) > 1 else (times[0] if times else None)
        county_m = re.search(r'[AP]M\s+(.+?)\s+\d{1,2}:\d{2}', lines1[0]) if lines1 else None
        result['county']        = county_m.group(1).strip() if county_m else None
        result['call_number']   = self._find(r'\b(\d{3})\s*$', lines1[0]) if lines1 else None
        result['total_pages']   = n_pages

        all_dates = re.findall(
            r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}', page1,
        )
        result['letting_date']  = all_dates[1] if len(all_dates) > 1 else (all_dates[0] if all_dates else None)
        result['contract_id']   = self._find(r'\b(DA\d{5}|MA\d{5}(?:\s*/\s*[A-Z]{2}\d{5})?)\b', page1)

        wbs_m = re.search(
            r'((?:\d{4}[A-Z]+\.\d+[\d.]*)'
            r'(?:[,\s]+\d{4}[A-Z]+\.\d+[\d.]*)*)', page1,
        )
        result['wbs_elements'] = (
            [e.strip() for e in re.split(r',\s*', wbs_m.group(1).replace('\n', ' '))]
            if wbs_m else []
        )
        if not result['wbs_elements']:
            wbs_simple = re.findall(r'\b([A-Z0-9]+\.[A-Z0-9.]+)\b', page1)
            result['wbs_elements'] = wbs_simple[:5] if wbs_simple else []

        result['tip_number']     = self._find(r'TIP NO\s+([A-Z][A-Z0-9\-]+)', page1) or 'None'
        result['project_number'] = self._find(r'FED AID NO\s+([^\n]+)', page1)
        result['total_miles']    = self._find(r'([\d.]+\s*MILES)', page1)

        fed_idx = next((i for i, l in enumerate(lines1) if 'FED AID NO' in l.upper()), None)
        result['description'] = (
            lines1[fed_idx + 1].strip()
            if fed_idx is not None and fed_idx + 1 < len(lines1) else None
        )
        loc_cand = (
            lines1[fed_idx + 2].strip()
            if fed_idx is not None and fed_idx + 2 < len(lines1) else None
        )
        result['location'] = (
            loc_cand
            if loc_cand and not re.search(r',\s+[A-Z]{2}\b|CONTRACTOR|BIDDER', loc_cand)
            else None
        )
        result['signed_date'] = self._find(r'(\d{2}/\d{2}/\d{4})\s*$', all_text)

        # Bidders
        bidders_m = re.search(
            r'BIDDERS IN ORDER\s+CONTRACT TOTAL\n(.*?)(?:\n\n|\Z)', last_page, re.DOTALL,
        )
        bidders = []
        if bidders_m:
            for line in bidders_m.group(1).strip().splitlines():
                m = re.match(r'^(.+?)\s+(\d+)\s+([\d,]+\.\d{2})\s*$', line.strip())
                if m:
                    bidders.append({
                        'rank':           int(m.group(2)),
                        'name':           m.group(1).strip(),
                        'contract_total': f'${m.group(3)}',
                    })
        result['bidders'] = bidders
        bidder_names = [b['name'] for b in bidders]

        # Line items
        item_map: dict = {}
        bidder_group = 0

        for page_num, page_text in enumerate(pages_text):
            item_lines = self._extract_item_lines(page_text)
            if not item_lines:
                continue

            first_item_num = item_lines[0][:4] if item_lines else '0000'
            if page_num > 0 and first_item_num == '0001':
                bidder_group += 1

            city_line = self._find_city_line(page_text)
            n_cols    = (
                len(re.findall(r',\s+[A-Z]{2}(?![A-Z0-9\-])', city_line, re.I))
                if city_line else 1
            )

            start        = bidder_group * 3
            page_bidders = (
                bidder_names[start: start + n_cols]
                if bidder_names else [f'Bidder {start + i + 1}' for i in range(n_cols)]
            )

            for raw_line in item_lines:
                parsed = self._parse_item_line(raw_line)
                if not parsed:
                    continue

                key = parsed['item_line']
                if key not in item_map:
                    item_map[key] = {
                        'item_line':   parsed['item_line'],
                        'item_number': parsed['item_number'],
                        'category':    parsed['category'],
                        'description': parsed['description'],
                        'quantity':    parsed['quantity'],
                        'unit':        parsed['unit'],
                        'bids':        [],
                    }

                prices  = parsed['_prices']
                is_lump = parsed['is_lump']

                if is_lump:
                    pairs = [('Lump Sum', p) for p in prices[:len(page_bidders)]]
                else:
                    it    = iter(prices)
                    pairs = list(zip(it, it))[:len(page_bidders)]

                for i, (up, ep) in enumerate(pairs):
                    bidder = page_bidders[i] if i < len(page_bidders) else f'Bidder {i + 1}'
                    item_map[key]['bids'].append({
                        'bidder':         bidder,
                        'unit_price':     up,
                        'extended_price': f'${ep}',
                    })

        result['line_items'] = sorted(item_map.values(), key=lambda x: x['item_line'])

        # Section subtotals
        section_totals = []
        for m in re.finditer(
            r'^(ROADWAY ITEMS|STRUCTURE ITEMS)\s+SUB-TOTAL\s+([\d,]+\.\d{2})'
            r'(?:\s+SUB-TOTAL\s+([\d,]+\.\d{2}))?(?:\s+SUB-TOTAL\s+([\d,]+\.\d{2}))?',
            all_text, re.MULTILINE,
        ):
            for grp in [m.group(2), m.group(3), m.group(4)]:
                if not grp:
                    continue
                match = next((b for b in bidders if b['contract_total'] == f'${grp}'), None)
                section_totals.append({
                    'section':  m.group(1),
                    'bidder':   match['name'] if match else 'Unknown',
                    'subtotal': f'${grp}',
                })
        result['section_totals'] = section_totals

        result['contract_totals'] = [
            {'bidder': b['name'], 'total': b['contract_total']} for b in bidders
        ]
        return result