import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class BasePDFExtractor(ABC):
    """
    Abstract base for PDF → structured-dict extractors.

    Subclasses implement `_extract(pdf_path: str) -> dict | list[dict]`.
    The public `extract()` method wraps that with error handling and
    attaches lightweight extraction metadata to the returned payload.
    """

    # Subclasses set this to a human-readable label, e.g. "AwardLetter"
    extractor_name: str = "Base"

    @abstractmethod
    def _extract(self, pdf_path: str) -> dict | list[dict]:
        """
        Parse *pdf_path* and return the raw structured data.

        Returns
        -------
        dict
            For single-document PDFs (award letter, bid tabs, ITB).
        list[dict]
            For multi-record PDFs where each page is a separate contract
            (Item C report).
        """

    def extract(self, pdf_path: str) -> dict:
        """
        Run extraction and wrap the result in a standard envelope:

        .. code-block:: json

            {
                "_meta": {
                    "extractor":   "AwardLetter",
                    "source_file": "path/to/file.pdf",
                    "extracted_at": "2025-01-01T12:00:00+00:00",
                    "status": "ok" // or "error"
                },
                "data": { ... } // raw extracted payload
            }

        On failure the envelope contains an ``"error"`` key and
        ``"data": null`` so downstream consumers can distinguish a
        clean empty result from an extraction failure.
        """

        pdf_path = str(pdf_path)
        meta: dict[str, Any] = {
            "extractor":    self.extractor_name,
            "source_file":  pdf_path,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            logger.info("[%s] extracting: %s", self.extractor_name, pdf_path)
            raw = self._extract(pdf_path)
            meta["status"] = "ok"
            logger.info("[%s] extraction succeeded", self.extractor_name)
            return {"_meta": meta, "data": raw}

        except FileNotFoundError:
            msg = f"PDF not found: {pdf_path}"
            logger.error("[%s] %s", self.extractor_name, msg)
            meta["status"] = "error"
            return {"_meta": meta, "data": None, "error": msg}

        except Exception as exc:  # noqa: BLE001
            msg = f"{type(exc).__name__}: {exc}"
            logger.exception("[%s] extraction failed: %s", self.extractor_name, msg)
            meta["status"] = "error"
            return {"_meta": meta, "data": None, "error": msg}

    def save_raw(
        self,
        data: dict | list,
        output_path: str | Path,
        *,
        indent: int = 2,
        ensure_ascii: bool = False,
    ) -> Path:
        """
        Persist *data* as pretty-printed JSON.

        Parameters
        ----------
        data:
            The dict (or list of dicts) to serialise — typically the
            full envelope returned by ``extract()``.
        output_path:
            Destination file path.  Parent directories are created
            automatically.
        indent:
            JSON indentation level (default 2).
        ensure_ascii:
            Passed straight to ``json.dumps`` (default False so UTF-8
            characters are preserved).

        Returns
        -------
        Path
            Resolved path to the written file.
        """

        out = Path(output_path).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)

        with out.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=indent, ensure_ascii=ensure_ascii, default=str)

        logger.info("[%s] raw JSON saved → %s", self.extractor_name, out)
        return out

    def extract_and_save(
        self,
        pdf_path: str | Path,
        output_dir: str | Path = ".",
        filename: str | None = None,
    ) -> tuple[dict, Path]:
        """
        Extract from *pdf_path* and immediately persist raw JSON.

        The default output filename is:
            ``<stem>_<extractor_name>_raw.json``

        Parameters
        ----------
        pdf_path:
            Source PDF.
        output_dir:
            Directory for the JSON file (created if absent).
        filename:
            Override the auto-generated filename.

        Returns
        -------
        tuple[dict, Path]
            ``(envelope, json_path)`` — the extraction envelope and the
            path to the saved JSON file.
        """

        pdf_path = Path(pdf_path)
        output_dir = Path(output_dir)

        envelope = self.extract(str(pdf_path))

        if filename is None:
            filename = f"{pdf_path.stem}_{self.extractor_name}_raw.json"

        out_path = self.save_raw(envelope, output_dir / filename)
        return envelope, out_path

    # Dunder helpers
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(extractor_name={self.extractor_name!r})"