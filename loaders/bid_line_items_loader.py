import logging
from typing import Optional

import pandas as pd

from db.postgresql_client import PostgreSQLClient

logger = logging.getLogger(__name__)

_SCHEMA = "ncdot"
_TABLE  = "bid_line_items"


_COLUMNS = [
    "run_id",
    "source_dir",
    "proposal_items_line_number",
    "items_number",
    "items_category",
    "items_description",
    "proposal_items_quantity",
    "items_unit",
    "item_section",
    "bids_rank",
    "vendors_name",
    "bid_items_unit_price",
    "bid_items_extension",
    "owners_state",
    "owners_name",
    "proposals_county",
    "proposals_contract_id",
    "proposals_project_number",
    "proposals_description",
    "lettings_date",
    "proposals_completion_date",
    "bids_value",
    "proposals_district_name",
    "proposals_call_number",
    "proposals_project_type",
    "proposals_cost_estimate",
]


class BidLineItemsLoader:
    """
    Inserts rows from a transformed DataFrame into ncdot.bid_line_items.

    Parameters
    ----------
    db_client:
        A ready-to-use PostgreSQLClient.  If omitted, one is created
        from environment variables automatically.
    """

    def __init__(self, db_client: Optional[PostgreSQLClient] = None) -> None:
        self._db = db_client or PostgreSQLClient()

    def load(
        self,
        df: pd.DataFrame,
        run_id: str,
        source_dir: str,
    ) -> int:
        """
        Insert all rows in *df* into the target table.

        Parameters
        ----------
        df:
            Output of ``PDFDataTransformer.transform()``.
        run_id:
            Airflow ``run_id`` string — stored for traceability.
        source_dir:
            Name of the inbox subdirectory this contract came from.

        Returns
        -------
        int
            Number of rows inserted.
        """
        if df.empty:
            logger.warning("[Loader] Empty DataFrame — nothing to load for %s", source_dir)
            return 0

        # Stamp traceability columns
        df = df.copy()
        df["run_id"]     = run_id
        df["source_dir"] = source_dir

        # Keep only the columns we care about, in the right order
        # Add any missing columns as empty strings so we never fail on schema gaps
        for col in _COLUMNS:
            if col not in df.columns:
                df[col] = ""

        df = df[_COLUMNS]

        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

        logger.info(
            "[Loader] Inserting %d rows into %s.%s (contract dir: %s)",
            len(rows), _SCHEMA, _TABLE, source_dir,
        )
        self._db.insert_rows(_SCHEMA, _TABLE, _COLUMNS, rows)
        logger.info("[Loader] Done — %d rows loaded", len(rows))

        return len(rows)
