import json
import re
import pandas as pd

from pathlib import Path


class PDFDataTransformer:
    """
    Merges raw data from all five PDF extractors and produces a flat
    DataFrame ready for loading into a database or analytics tool.

    Usage
    -----
        transformer = PDFDataTransformer()
        df = transformer.transform(
            award_letter=award_envelope,
            bid_tabs=bid_tabs_envelope,
            invitation_to_bid=itb_envelope,
            item_c_report=item_c_envelope,
            bids_as_read=bar_envelope,
            contract_id="DA00539",
        )
    """

    def __init__(self, raw_json_dir: str | Path | None = None):
        self._raw_json_dir = Path(raw_json_dir) if raw_json_dir else None

    def transform(
        self,
        award_letter:      dict | None = None,
        bid_tabs:          dict | None = None,
        invitation_to_bid: dict | None = None,
        item_c_report:     dict | None = None,
        bids_as_read:      dict | None = None,
        contract_id:       str | None = None,
    ) -> pd.DataFrame:

        award_letter = award_letter or self._load("award_letter")
        bid_tabs = bid_tabs or self._load("bid_tabs")
        invitation_to_bid = invitation_to_bid or self._load("invitation_to_bid")
        item_c_report = item_c_report or self._load("item_c_report")
        bids_as_read = bids_as_read or self._load("bids_as_read")

        al  = self._data(award_letter)
        bt  = self._data(bid_tabs)
        itb = self._data(invitation_to_bid)
        icr = self._data(item_c_report)
        bar = self._data(bids_as_read)

        # Resolve contract_id - prefer explicitly passed, then coalesce from sources
        resolved_contract_id = (
            contract_id
            or self._coalesce(al, bt, itb, key='contract_id')
        )

        # Try Item C first — fall back to Bids As Read if Item C doesn't have this contract
        icr_page = self._match_paged_source(icr, resolved_contract_id)
        bar_page = self._match_paged_source(bar, resolved_contract_id)
        print(f'bar_page: {bar_page}')

        # Use whichever source has data — Item C takes priority
        meta_source = icr_page if icr_page else bar_page

        #  Build line-items table
        df = self._build_line_items_table(bt)

        if df.empty:
            return df

        # Resolve proposal-level fields
        df['owners_state']             = self._coalesce(itb, meta_source, al,  key='state')
        df['owners_name']              = self._coalesce(itb, meta_source, al,  key='owner')
        df['proposals_county']         = self._coalesce(al, bt, itb, meta_source, key='county')
        df['proposals_contract_id']    = resolved_contract_id
        df['proposals_project_number'] = self._coalesce(al, bt, meta_source,   key='project_number')
        df['proposals_description']    = self._coalesce(itb, al,               key='proposal_description')
        df['lettings_date']            = self._coalesce(bt, itb, al, meta_source, key='letting_date')
        df['proposals_completion_date'] = self._coalesce(itb, meta_source,     key='completion_date')
        df['bids_value']               = al.get('bid_value', '')
        df['proposals_district_name']  = self._coalesce(itb, meta_source,      key='district')
        df['proposals_call_number']    = bt.get('call_number', '')

        # Item C exclusive fields - fall back to Bids As Read engineers_estimate
        df['proposals_project_type']  = icr_page.get('type_of_work', '')
        df['proposals_cost_estimate'] = (
            icr_page.get('estimate', '')
            or bar_page.get('engineers_estimate', '')
            or bar_page.get('corrected_engineers_estimate', '')
        )

        return df

    def _load(self, token: str) -> dict | None:
        """Load the first JSON file in raw_json_dir whose name contains *token*."""
        if self._raw_json_dir is None:
            return None
        matches = [
            f for f in self._raw_json_dir.iterdir()
            if token in f.name and f.suffix == '.json'
        ]
        if not matches:
            return None
        with matches[0].open('r', encoding='utf-8') as fh:
            return json.load(fh)

    def _data(self, envelope: dict | None) -> dict | list:
        """
        Unwrap the extraction envelope.
        Returns a list for multi-page extractors (Item C, Bids As Read),
        a dict for single-document extractors, or empty dict on failure.
        """
        if not envelope:
            return {}
        if envelope.get('_meta', {}).get('status') != 'ok':
            return {}
        return envelope.get('data') or {}

    def _coalesce(self, *sources: dict, key: str) -> str:
        """Return the first non-empty value for *key* across *sources*."""
        for src in sources:
            if not isinstance(src, dict):
                continue
            val = src.get(key, '')
            if val:
                return val
        return ''

    def _match_paged_source(self, source: dict | list, contract_id: str) -> dict:
        """
        For multi-page sources (Item C report, Bids As Read) find the page
        matching the given contract_id.

        Handles dual IDs like "12107176 / MA00004" — the raw contract_id in
        the source is matched if the needle appears anywhere within it
        (after normalising whitespace and case). This means grouping key
        "12107176" will match a page whose contract_id is "12107176 / MA00004".
        """
        if not contract_id or not source:
            return {}
        needle = contract_id.strip().upper()
        if isinstance(source, list):
            for page in source:
                raw_cid = (page.get('contract_id') or '').strip().upper()
                # Normalize slash-separated dual IDs for comparison
                raw_cid_normalized = re.sub(r'\s*/\s*', ' ', raw_cid)
                if needle == raw_cid_normalized or needle in raw_cid_normalized.split():
                    return page
            return {}
        return source if isinstance(source, dict) else {}

    def _build_line_items_table(self, bt: dict) -> pd.DataFrame:
        """Expand bid_tabs line items into one row per item x bidder."""
        if not bt or 'line_items' not in bt:
            return pd.DataFrame()

        bidder_rank = {b['name']: b['rank'] for b in bt.get('bidders', [])}

        rows = []
        for item in bt['line_items']:
            base = {
                'proposal_items_line_number': item['item_line'],
                'items_number':               item['item_number'],
                'items_category':             item['category'],
                'items_description':          item['description'],
                'proposal_items_quantity':    item['quantity'],
                'items_unit':                 item['unit'] or '',
                'item_section':               item.get('section', ''),
            }
            for bid in item['bids']:
                rows.append({
                    **base,
                    'bids_rank':            bidder_rank.get(bid['bidder'], ''),
                    'vendors_name':         bid['bidder'],
                    'bid_items_unit_price': bid['unit_price'],
                    'bid_items_extension':  bid['extended_price'],
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(
                ['proposal_items_line_number', 'bids_rank']
            ).reset_index(drop=True)
        return df