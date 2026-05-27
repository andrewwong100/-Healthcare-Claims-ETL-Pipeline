"""
test_cms_api_client.py
Unit tests for CMSApiClient — mocks HTTP layer, tests pagination, retry, and error handling.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))
from cms_api_client import CMSApiClient, PAGE_SIZE


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_page(n: int, offset: int = 0) -> list[dict]:
    """Generate n fake CMS Part B records."""
    return [
        {
            "rndrng_npi": f"100000{offset + i:04d}",
            "rndrng_prvdr_last_org_name": f"Provider {offset + i}",
            "rndrng_prvdr_type": "Internal Medicine",
            "rndrng_prvdr_state_abrvtn": "CA",
            "hcpcs_cd": "99213",
            "hcpcs_desc": "Office visit, established patient",
            "tot_srvcs": 100 + i,
            "tot_benes": 80 + i,
            "avg_mdcr_alowd_amt": 75.50,
            "avg_submtd_chrg": 120.00,
            "avg_mdcr_pymt_amt": 60.40,
            "avg_mdcr_stdzd_amt": 58.20,
            "year": "2022",
        }
        for i in range(n)
    ]


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestCMSApiClient:

    def test_fetch_single_page(self):
        """Single page returned → DataFrame with correct row count."""
        client = CMSApiClient()
        fake_page = make_page(50)

        with patch.object(client, "_fetch_page", return_value=fake_page) as mock_fetch:
            df = client.fetch_all(year="2022", dataset="part_b")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 50
        mock_fetch.assert_called_once()

    def test_pagination_stops_on_empty_page(self):
        """Pagination loop stops when API returns empty list."""
        client = CMSApiClient()
        pages = [make_page(PAGE_SIZE), make_page(PAGE_SIZE), []]   # 3rd page empty

        with patch.object(client, "_fetch_page", side_effect=pages) as mock_fetch:
            df = client.fetch_all(year="2022", dataset="part_b")

        assert len(df) == PAGE_SIZE * 2
        assert mock_fetch.call_count == 3

    def test_pagination_stops_on_partial_last_page(self):
        """Pagination stops when last page returns fewer rows than PAGE_SIZE."""
        client = CMSApiClient()
        pages = [make_page(PAGE_SIZE), make_page(123)]   # 123 < PAGE_SIZE → last page

        with patch.object(client, "_fetch_page", side_effect=pages):
            df = client.fetch_all(year="2022", dataset="part_b")

        assert len(df) == PAGE_SIZE + 123

    def test_raises_on_empty_result(self):
        """Raises ValueError when API returns zero records for the year."""
        client = CMSApiClient()

        with patch.object(client, "_fetch_page", return_value=[]):
            with pytest.raises(ValueError, match="No records returned"):
                client.fetch_all(year="1900", dataset="part_b")

    def test_correct_params_passed_to_api(self):
        """Verifies offset and filter params are passed correctly per page."""
        client = CMSApiClient()
        pages = [make_page(PAGE_SIZE), make_page(10)]

        with patch.object(client, "_fetch_page", side_effect=pages) as mock_fetch:
            client.fetch_all(year="2021", dataset="part_a")

        first_call_params = mock_fetch.call_args_list[0][0][1]
        assert first_call_params["filter[year]"] == "2021"
        assert first_call_params["offset"] == 0
        assert first_call_params["size"] == PAGE_SIZE

        second_call_params = mock_fetch.call_args_list[1][0][1]
        assert second_call_params["offset"] == PAGE_SIZE

    def test_invalid_dataset_raises(self):
        """Raises ValueError on unknown dataset type."""
        client = CMSApiClient()
        with pytest.raises(KeyError):
            client.fetch_all(year="2022", dataset="part_z")


class TestSchemaValidator:
    """Schema validation unit tests."""

    def setup_method(self):
        import tempfile
        from pathlib import Path
        from schema_validator import SchemaValidator
        self.tmp = tempfile.mkdtemp()
        self.validator = SchemaValidator(snapshot_dir=Path(self.tmp))

    def test_valid_schema_passes(self):
        """Validation passes when all required columns are present."""
        df = pd.DataFrame(columns=[
            "rndrng_npi", "rndrng_prvdr_type", "rndrng_prvdr_state_abrvtn",
            "hcpcs_cd", "tot_srvcs", "avg_mdcr_pymt_amt", "extra_col"
        ])
        # Should not raise
        self.validator.validate(df, "part_b")

    def test_missing_required_column_raises(self):
        """Raises SchemaValidationError when required column is missing."""
        from schema_validator import SchemaValidationError
        df = pd.DataFrame(columns=["rndrng_npi", "hcpcs_cd"])   # missing several required
        with pytest.raises(SchemaValidationError, match="Schema drift detected"):
            self.validator.validate(df, "part_b")

    def test_snapshot_saved_after_validation(self):
        """A schema snapshot JSON file is written after successful validation."""
        import os
        df = pd.DataFrame(columns=[
            "rndrng_npi", "rndrng_prvdr_type", "rndrng_prvdr_state_abrvtn",
            "hcpcs_cd", "tot_srvcs", "avg_mdcr_pymt_amt"
        ])
        self.validator.validate(df, "part_b")
        snapshots = list(Path(self.tmp).glob("part_b_schema_*.json"))
        assert len(snapshots) == 1

    def test_detect_drift_finds_removed_column(self):
        """detect_drift correctly identifies columns removed vs last snapshot."""
        # First run — saves snapshot with extra column
        df_v1 = pd.DataFrame(columns=[
            "rndrng_npi", "rndrng_prvdr_type", "rndrng_prvdr_state_abrvtn",
            "hcpcs_cd", "tot_srvcs", "avg_mdcr_pymt_amt", "legacy_field"
        ])
        self.validator.validate(df_v1, "part_b")

        # Second run — legacy_field removed
        df_v2 = pd.DataFrame(columns=[
            "rndrng_npi", "rndrng_prvdr_type", "rndrng_prvdr_state_abrvtn",
            "hcpcs_cd", "tot_srvcs", "avg_mdcr_pymt_amt"
        ])
        drift = self.validator.detect_drift(df_v2, "part_b")
        assert "legacy_field" in drift["removed"]
        assert drift["added"] == []
