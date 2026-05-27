"""
schema_validator.py
Detects schema drift in CMS API responses before they hit the warehouse.
Raises SchemaValidationError on missing required columns; logs warnings on unexpected new ones.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Required columns by dataset — subset of expected schema (non-nullable critical fields)
REQUIRED_COLUMNS = {
    "part_a": [
        "rndrng_prvdr_ccn",
        "rndrng_prvdr_state_abrvtn",
        "drg_cd",
        "tot_dschrgs",
        "avg_mdcr_pymt_amt",
    ],
    "part_b": [
        "rndrng_npi",
        "rndrng_prvdr_type",
        "rndrng_prvdr_state_abrvtn",
        "hcpcs_cd",
        "tot_srvcs",
        "avg_mdcr_pymt_amt",
    ],
}

SNAPSHOT_DIR = Path("/opt/airflow/schema_snapshots")


class SchemaValidationError(Exception):
    """Raised when required columns are missing from the CMS API response."""
    pass


class SchemaValidator:
    """
    Validates incoming DataFrames against known CMS schemas.
    Saves schema snapshots to disk so drift can be tracked over time.

    Usage:
        validator = SchemaValidator()
        validator.validate(df, dataset_type="part_b")  # raises if columns missing
    """

    def __init__(self, snapshot_dir: Optional[Path] = None):
        self.snapshot_dir = snapshot_dir or SNAPSHOT_DIR
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def validate(self, df: pd.DataFrame, dataset: str) -> None:
        """
        Validate df against required columns for dataset type.

        Args:
            df: DataFrame from CMS API
            dataset: "part_a" or "part_b"

        Raises:
            SchemaValidationError: if any required column is missing
        """
        if dataset not in REQUIRED_COLUMNS:
            raise ValueError(f"Unknown dataset type: {dataset}")

        required = set(REQUIRED_COLUMNS[dataset])
        actual = set(df.columns.str.lower())

        missing = required - actual
        unexpected = actual - required   # new columns CMS added — log, don't fail

        if missing:
            raise SchemaValidationError(
                f"Schema drift detected in {dataset}! "
                f"Missing required columns: {sorted(missing)}"
            )

        if unexpected:
            logger.warning(
                f"New unexpected columns in {dataset} (non-breaking): {sorted(unexpected)}"
            )

        # Save snapshot for audit trail
        self._save_snapshot(df.columns.tolist(), dataset)
        logger.info(f"Schema validation passed for {dataset} ({len(df.columns)} columns)")

    def detect_drift(self, df: pd.DataFrame, dataset: str) -> dict:
        """
        Compare current schema against the last saved snapshot.

        Returns:
            dict with keys 'added', 'removed' (lists of column names)
        """
        snapshot = self._load_last_snapshot(dataset)
        if snapshot is None:
            return {"added": [], "removed": [], "note": "No prior snapshot found"}

        current = set(df.columns.str.lower())
        prior = set(c.lower() for c in snapshot)

        return {
            "added": sorted(current - prior),
            "removed": sorted(prior - current),
        }

    def _save_snapshot(self, columns: list[str], dataset: str) -> None:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self.snapshot_dir / f"{dataset}_schema_{ts}.json"
        path.write_text(json.dumps({"columns": columns, "captured_at": ts}, indent=2))
        logger.debug(f"Schema snapshot saved → {path}")

    def _load_last_snapshot(self, dataset: str) -> Optional[list[str]]:
        snapshots = sorted(self.snapshot_dir.glob(f"{dataset}_schema_*.json"))
        if not snapshots:
            return None
        data = json.loads(snapshots[-1].read_text())
        return data.get("columns", [])
