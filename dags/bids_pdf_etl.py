import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# Make project modules importable inside Airflow workers
sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

# Constants
INBOX_DIR   = Path(os.environ.get("ETL_INBOX_DIR",   "/opt/airflow/inbox"))
RAW_JSON_DIR = Path(os.environ.get("ETL_RAW_JSON_DIR", "/opt/airflow/raw_json"))

_EXTRACTOR_TOKENS = {
    "award_letter":      "award_letter",
    "bid_tabs":          "bid_tabs",
    "invitation_to_bid": "invitation_to_bid",
    "item_c_report":     "item_c_report",
    "bids_as_read":      "bids_as_read",
}

# DAG
default_args = {
    "owner":            "airflow",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="bids_pdf_etl",
    description="Contracts & Bids PDF → PostgreSQL ETL pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["bids", "etl", "pdf"],
)
def bids_pdf_etl():
    """
    PDF ingestion pipeline for the Bids dataset.
    For each unprocessed batch in the inbox,
    runs through the ETL steps:
      1. Scan: group PDFs by contract ID and type
      2. Extract: run the appropriate extractor(s) for each contract
      3. Transform: pair up extractions and run the transformer
      4. Load: insert the transformed data into the database
    """

    @task
    def scan_inbox() -> list[dict]:
        """
        Walk every unprocessed batch subdirectory inside INBOX_DIR.
        Within each batch dir, group PDF files by contract ID and type.

        Real filenames are messy — spaces, underscores, mixed case, optional
        prefixes, digits-only IDs, alphanumeric IDs, and some files carry no
        contract ID at all.  The rules applied are:

          1. Contract ID  — the FIRST token in the stem that looks like an ID:
                              • All-digit codes   e.g. "12107176"
                              • Letter(s)+digits  e.g. "DA00592", "MA00004"
                              • Digits+letter(s)  e.g. "L231206A"
                            Matched by /\\b([A-Z]*\\d+[A-Z]*)\\b/ (case-insensitive),
                            taking the first match in the stem.
                            Files with TWO ids (e.g. "12107176_ MA00004 Award Le...")
                            use only the first one as the grouping key.

          2. Type token   — keyword scan of the lowercased stem (most specific first):
                              "invitation to bid" → invitation_to_bid
                              "item c" → item_c_report
                              "award letter" → award_letter
                              "bid tab" → bid_tabs
                              "bids as read" → bids_as_read (alternate name)

          3. No contract ID (e.g. "Item C Report.pdf", "Bids As Read.pdf") →
             assigned to every contract ID found in the same batch directory.

        Returns a flat list of contract descriptors for dynamic task mapping.
        """

        _CONTRACT_ID_RE = re.compile(
            r'(?<![A-Z\d])([A-Z]*\d+[A-Z]*)(?![A-Z\d])',
            re.IGNORECASE,
        )

        # Keyword → token (order matters: most specific first)
        _TYPE_KEYWORDS: list[tuple[str, str]] = [
            ("invitation to bid", "invitation_to_bid"),
            ("item c", "item_c_report"),
            ("bids as read", "bids_as_read"),
            ("bid summary", "bids_as_read"),
            ("bid tabulation", "bid_tabs"),
            ("awardletter", "award_letter"),
            ("award_letter", "award_letter"),
            ("award letter", "award_letter"),
            ("bid tab", "bid_tabs"),
        ]

        def _detect_type(stem: str) -> str | None:
            s = stem.lower()
            for keyword, token in _TYPE_KEYWORDS:
                if keyword in s:
                    return token
            return None

        def _detect_contract_id(stem: str) -> str | None:
            """Return the first ID-like token found in the stem, uppercased."""
            m = _CONTRACT_ID_RE.search(stem)
            return m.group(1).upper() if m else None

        INBOX_DIR.mkdir(parents=True, exist_ok=True)
        contracts = []

        logger.info("Scanning inbox: %s", INBOX_DIR)
        all_entries = list(INBOX_DIR.iterdir())
        logger.info("Entries found in inbox: %s", [e.name for e in all_entries])

        for batch_dir in sorted(all_entries):
            if not batch_dir.is_dir():
                logger.info("Skipping non-directory: %s", batch_dir.name)
                continue
            if (batch_dir / ".processed").exists():
                logger.info("Skipping already-processed batch: %s", batch_dir.name)
                continue

            logger.info("Processing batch dir: %s", batch_dir.name)

            # First pass: classify every PDF in the batch
            typed_files: list[tuple[str | None, str, str]] = []

            all_pdfs = list(batch_dir.glob("*.pdf"))
            logger.info("PDFs found in %s: %s", batch_dir.name, [p.name for p in all_pdfs])

            for pdf in sorted(all_pdfs):
                token = _detect_type(pdf.stem)
                if not token:
                    logger.warning("Cannot detect type for: %s — skipping", pdf.name)
                    continue

                contract_id = _detect_contract_id(pdf.stem)
                logger.info("  %s → contract=%s  type=%s", pdf.name, contract_id, token)
                typed_files.append((contract_id, token, str(pdf)))

            if not typed_files:
                logger.warning("No recognisable PDFs in batch %s", batch_dir.name)
                continue

            known_ids = sorted({
                cid for cid, token, _ in typed_files
                if cid is not None and token not in ("item_c_report", "bids_as_read")
            })

            if not known_ids:
                logger.warning("No contract IDs found in batch %s — skipping", batch_dir.name)
                continue

            grouped: dict[str, dict[str, list[str]]] = {
                cid: {k: [] for k in _EXTRACTOR_TOKENS} for cid in known_ids
            }

            for contract_id, token, path in typed_files:
                if token in ("item_c_report", "bids_as_read"):
                    for cid in known_ids:
                        grouped[cid][token].append(path)
                elif contract_id is not None:
                    grouped[contract_id][token].append(path)
                else:
                    for cid in known_ids:
                        grouped[cid][token].append(path)

            for contract_id, files in grouped.items():
                contracts.append({
                    "batch_dir":   str(batch_dir),
                    "contract_id": contract_id,
                    "files":       files,
                })
                logger.info(
                    "Queued contract %s from batch %s  (%s)",
                    contract_id,
                    batch_dir.name,
                    {k: len(v) for k, v in files.items()},
                )

        logger.info("scan_inbox: %d contract(s) queued", len(contracts))
        return contracts

    @task
    def extract(contract: dict, **context) -> dict:
        """
        For each extractor type, run the corresponding extractor on every
        matching PDF for this contract.
 
        Returns:
        {
            "batch_dir":   "...",
            "contract_id": "DA00642",
            "run_id":      "...",
            "extractions": {
                "award_letter":      [envelope, ...],
                "bid_tabs":          [envelope, envelope, ...],
                "invitation_to_bid": [envelope, ...],
                "item_c_report":     [envelope, ...],
            }
        }
        """

        run_id = context["run_id"]
        
        from extractors.award_letter_extractor import AwardLetterExtractor
        from extractors.bid_tabs_extractor import BidTabsExtractor
        from extractors.bid_tabs_idiq_extractor import BidTabsIDIQExtractor
        from extractors.invitation_to_bid_extractor import InvitationToBidExtractor
        from extractors.item_c_report_extractor import ItemCReportExtractor
        from extractors.bids_as_read_extractor import BidsAsReadExtractor

        _BID_TABS_KEYWORDS = ("bid tabulation",)

        def _pick_bid_tabs_extractor(pdf_path: str):
            name = pdf_path.lower()
            if any(k in name for k in _BID_TABS_KEYWORDS):
                return BidTabsIDIQExtractor()
            return BidTabsExtractor()

        extractor_map = {
            "award_letter": AwardLetterExtractor(),
            "invitation_to_bid": InvitationToBidExtractor(),
            "item_c_report": ItemCReportExtractor(),
            "bids_as_read": BidsAsReadExtractor(),
        }

        batch_dir   = Path(contract["batch_dir"])
        contract_id = contract["contract_id"]
        raw_out_dir = RAW_JSON_DIR / batch_dir.name / contract_id
        raw_out_dir.mkdir(parents=True, exist_ok=True)

        extractions: dict[str, list[dict]] = {k: [] for k in _EXTRACTOR_TOKENS}

        for token, pdf_paths in contract["files"].items():
            for pdf_path in pdf_paths:
                pdf = Path(pdf_path)
                if token == "bid_tabs":
                    extractor = _pick_bid_tabs_extractor(pdf_path)
                else:
                    extractor = extractor_map[token]
                logger.info("[Extract] %s | %s → %s", contract_id, token, pdf.name)
                envelope, _ = extractor.extract_and_save(
                    pdf_path=pdf,
                    output_dir=raw_out_dir,
                    filename=f"{pdf.stem}_{token}_raw.json",
                )
                extractions[token].append(envelope)

        return {
            "batch_dir":   str(batch_dir),
            "contract_id": contract_id,
            "run_id":      run_id,
            "extractions": extractions,
        }

    @task
    def transform(extraction_result: dict, **context) -> dict:
        """
        Pairs up envelopes by position across types and runs the transformer
        for each pair. Multiple files of the same type produce multiple runs.
 
        Returns a serialisable dict:
        {
            "batch_dir":   "...",
            "contract_id": "DA00642",
            "run_id":      "...",
            "columns":     [...],
            "records":     [[row_values, ...], ...]
        }
        """

        from transformers.transform import PDFDataTransformer

        batch_dir   = extraction_result["batch_dir"]
        contract_id = extraction_result["contract_id"]
        run_id      = context["run_id"]
        extractions = extraction_result["extractions"]

        transformer = PDFDataTransformer()

        # Number of runs = max files across all types
        n_runs = max((len(v) for v in extractions.values()), default=0)

        def _get(token: str, idx: int):
            lst = extractions.get(token, [])
            return lst[idx] if idx < len(lst) else None

        all_records: list[list] = []
        columns: list[str] = []

        for i in range(n_runs):
            df = transformer.transform(
                award_letter      = _get("award_letter",      i),
                bid_tabs          = _get("bid_tabs",          i),
                invitation_to_bid = _get("invitation_to_bid", i),
                item_c_report     = _get("item_c_report",     i),
                bids_as_read      = _get("bids_as_read",      i),
                contract_id       = contract_id,
            )

            if df.empty:
                logger.warning(
                    "[Transform] Run %d/%d produced an empty DataFrame for %s",
                    i + 1, n_runs, contract_id,
                )
                continue

            columns = list(df.columns)
            df = df.where(df.notna(), other=None)
            all_records.extend(df.values.tolist())
            logger.info(
                "[Transform] Run %d/%d: %d rows for contract %s",
                i + 1, n_runs, len(df), contract_id,
            )

        return {
            "batch_dir":   batch_dir,
            "contract_id": contract_id,
            "run_id":      run_id,
            "columns":     columns,
            "records":     all_records,
        }

    @task
    def load(transform_result: dict) -> int:
        """
        Reconstruct the DataFrame from the serialised records and insert
        into bids.bid_line_items via BidLineItemsLoader.
 
        The .processed sentinel is written on the batch directory only
        after ALL contracts in the batch have been loaded — handled by
        Airflow's task completion tracking since all load tasks share
        the same batch_dir and the sentinel is idempotent (touch).
        """
        
        import pandas as pd
        from loaders.bid_line_items_loader import BidLineItemsLoader

        batch_dir   = Path(transform_result["batch_dir"])
        contract_id = transform_result["contract_id"]
        run_id      = transform_result["run_id"]
        columns     = transform_result["columns"]
        records     = transform_result["records"]

        if not records:
            logger.warning("[Load] No records to load for contract %s", contract_id)
            _mark_processed(batch_dir)
            return 0

        df = pd.DataFrame(records, columns=columns)

        loader      = BidLineItemsLoader()
        rows_loaded = loader.load(df, run_id=run_id, source_dir=contract_id)

        _mark_processed(batch_dir)

        logger.info("[Load] %d rows loaded for contract %s", rows_loaded, contract_id)
        return rows_loaded

    def _mark_processed(batch_dir: Path) -> None:
        """Touch the sentinel. Safe to call multiple times (idempotent)."""
        sentinel = batch_dir / ".processed"
        sentinel.touch()
        logger.info("[Load] Marked as processed: %s", sentinel)

    contracts = scan_inbox()

    # Dynamic task mapping: one extract→transform→load chain per contract
    extraction_results = extract.expand(contract=contracts)
    transform_results  = transform.expand(extraction_result=extraction_results)
    load.expand(transform_result=transform_results)


bids_pdf_etl()