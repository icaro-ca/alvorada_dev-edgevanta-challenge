import re
import pdfplumber

from extractors.base_extractor import BasePDFExtractor


class BidTabsIDIQExtractor(BasePDFExtractor):
    """
    Extracts structured data from NC DOT Bid Tabulations (IDIQ) PDFs.

    Returned ``data`` keys match BidTabsExtractor output
    -------------------------------------------------------
    contract_id, letting_date, tip_number, wbs_element, county,
    description, bidders, line_items, contract_totals, section_totals
    """

    extractor_name = "BidTabsIDIQExtractor"

    def _clean_num(self, s: str) -> str:
        """
        Remove PDF-renderer spaces injected inside numbers.
        '1 2,000.00' → '12,000.00'   '4 ,000.00' → '4,000.00'
        Also strips leading '$' and whitespace.
        """
        s = s.strip().lstrip('$').strip()
        s = re.sub(r'(?<=\d)\s+(?=[\d,])', '', s)
        s = re.sub(r'(?<=\d)\s+(?=\.)',    '', s)
        return s.strip()

    def _parse_header(self, all_text: str) -> dict:
        """Extract contract-level metadata from the text header block."""
        meta = {}

        # contract_id — may be 'MA00015 / PR24587' or plain '12089199'
        m = re.search(r'Contract No\.:\s*(.+?)(?:\s+Date:)', all_text)
        meta['contract_id'] = m.group(1).strip() if m else None

        m = re.search(r'Date:\s*([\d/]+)', all_text)
        meta['letting_date'] = m.group(1).strip() if m else None

        m = re.search(r'TIP No\.:\s*(.+)', all_text)
        meta['tip_number'] = m.group(1).strip() if m else None

        m = re.search(r'WBS No\.:\s*(.+)', all_text)
        meta['wbs_element'] = m.group(1).strip() if m else None

        m = re.search(r'County:\s*(.+)', all_text)
        county_raw = m.group(1).strip() if m else None
        meta['county'] = re.sub(
            r',?\s*Counti?(?:es|y)?\s*$', '', county_raw or '', flags=re.IGNORECASE
        ).strip() or None

        m = re.search(r'Description:\s*(.+?)(?:\s+Upon Request)', all_text, re.DOTALL)
        meta['description'] = re.sub(r'\s+', ' ', m.group(1)).strip() if m else None

        return meta

    def _parse_bidder_names(self, first_row: list) -> list[str]:
        """Extract bidder names from the header row of the first table."""
        return [
            cell.strip() for cell in first_row
            if cell and cell.strip()
            and cell.strip() not in ('UNIT\nPRICE', 'AMOUNT', 'UNIT PRICE')
        ]

    def _parse_line_items(self, tables: list, bidder_names: list) -> tuple[list, list]:
        """
        Parse all item rows from pdfplumber tables across all pages.
        Returns (line_items, contract_totals).

        Column layout:
          [line_no, category, description, qty, unit,
           unit_price_1, amount_1, unit_price_2, amount_2, ...]
        """
        totals_by_bidder = {name: None for name in bidder_names}
        line_items = []

        for table in tables:
            for row in table:
                if not row:
                    continue

                # ── Total row ─────────────────────────────────────────────
                total_cells = [
                    c for c in row
                    if c and re.match(r'Total:', str(c), re.IGNORECASE)
                ]
                if total_cells:
                    for idx, name in enumerate(bidder_names):
                        if idx < len(total_cells):
                            val = re.sub(r'Total:\s*\$?\s*', '', total_cells[idx]).strip()
                            totals_by_bidder[name] = self._clean_num(val)
                    continue

                # Skip rows with no line number in col 0
                if not row[0]:
                    continue
                cell0 = str(row[0]).strip()

                # Skip header / label rows
                if not re.match(r'^\d+$', cell0):
                    continue

                # ── Data row ─────────────────────────────────────────────
                item_line = cell0.zfill(4)
                category  = (row[1] or '').strip()
                desc      = re.sub(r'\s+', ' ', (row[2] or '').strip())
                qty_raw   = (row[3] or '').strip()
                unit      = (row[4] or '').strip() or None
                qty       = re.sub(r'\s+', '', qty_raw) if qty_raw else None

                bids = []
                for i, name in enumerate(bidder_names):
                    up_col  = 5 + i * 2
                    ext_col = 6 + i * 2
                    up_raw  = row[up_col]  if up_col  < len(row) else None
                    ext_raw = row[ext_col] if ext_col < len(row) else None

                    up  = self._clean_num(up_raw)  if up_raw  else None
                    ext = self._clean_num(ext_raw) if ext_raw else None

                    is_lump = (
                        (qty and qty.replace(' ', '').lower() in ('lumpsum', 'ls'))
                        or (up and up.lower().replace(' ', '') == 'lumpsum')
                    )
                    if is_lump:
                        up = 'Lump Sum'

                    bids.append({
                        'bidder':         name,
                        'unit_price':     up,
                        'extended_price': f'${ext}' if ext and not ext.startswith('$') else ext,
                    })

                line_items.append({
                    'item_line':   item_line,
                    'item_number': None,
                    'category':    category,
                    'description': desc,
                    'quantity':    qty,
                    'unit':        unit,
                    'bids':        bids,
                })

        contract_totals = [
            {
                'bidder': name,
                'total':  f'${v}' if v and not v.startswith('$') else v,
            }
            for name, v in totals_by_bidder.items()
        ]

        return line_items, contract_totals

    def _extract(self, pdf_path: str) -> dict:
        '''
        Parse the Bid Tabulations (IDIQ) PDF and return all structured fields.
 
        Unlike ``BidTabsExtractor``, which processes free-form text columns,
        this extractor relies on pdfplumber's table detection because IDIQ
        PDFs render data inside proper table cells. 
        '''
        with pdfplumber.open(pdf_path) as pdf:
            pages_text = [p.extract_text() or '' for p in pdf.pages]
            all_tables = [p.extract_tables() for p in pdf.pages]

        all_text   = '\n'.join(pages_text)
        flat_tables = [t for page_tables in all_tables for t in page_tables]

        result = self._parse_header(all_text)

        first_table = next(iter(flat_tables), None)
        if not first_table:
            raise ValueError("No table found in PDF")

        bidder_names    = self._parse_bidder_names(first_table[0])
        result['bidders'] = [
            {'rank': i + 1, 'name': name, 'contract_total': None}
            for i, name in enumerate(bidder_names)
        ]

        line_items, contract_totals = self._parse_line_items(flat_tables, bidder_names)

        totals_map = {ct['bidder']: ct['total'] for ct in contract_totals}
        for b in result['bidders']:
            b['contract_total'] = totals_map.get(b['name'])

        result['line_items']      = line_items
        result['contract_totals'] = contract_totals
        result['section_totals']  = []

        return result