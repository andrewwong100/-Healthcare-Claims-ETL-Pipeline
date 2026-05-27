"""
cms_api_client.py
Fetches CMS Medicare Part A & B public datasets with pagination and retry logic.
"""
import time
import logging
from typing import Literal

import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

# CMS Open Data API — public, no key required for basic access
CMS_DATASET_IDS = {
    "part_a": "9767cb68-8ea9-4f0b-8179-9431abc89f11",   # Inpatient claims
    "part_b": "02c6-7d29-4b8e-9c3a-1f2e5d6a7b0c",       # Physician/supplier
}

EXPECTED_SCHEMAS = {
    "part_a": {
        "rndrng_prvdr_ccn", "rndrng_prvdr_org_name", "rndrng_prvdr_state_abrvtn",
        "drg_cd", "drg_desc", "tot_dschrgs", "avg_submtd_cvrd_chrg",
        "avg_ttl_pymt_amt", "avg_mdcr_pymt_amt"
    },
    "part_b": {
        "rndrng_npi", "rndrng_prvdr_last_org_name", "rndrng_prvdr_type",
        "rndrng_prvdr_state_abrvtn", "hcpcs_cd", "hcpcs_desc",
        "tot_srvcs", "tot_benes", "avg_mdcr_alowd_amt", "avg_mdcr_pymt_amt"
    },
}

PAGE_SIZE = 5000


class CMSApiClient:
    """
    Fetches CMS Medicare public data with pagination, schema validation, and retries.

    Usage:
        client = CMSApiClient()
        df = client.fetch_all(year="2022", dataset="part_b")
    """

    BASE_URL = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"

    def __init__(self, timeout: int = 30, max_pages: int = 500):
        self.timeout = timeout
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        stop=stop_after_attempt(5),
    )
    def _fetch_page(self, url: str, params: dict) -> list[dict]:
        """Fetch a single page from the CMS API with retry on transient errors."""
        response = self.session.get(url, params=params, timeout=self.timeout)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            logger.warning(f"Rate limited — waiting {retry_after}s")
            time.sleep(retry_after)
            raise requests.ConnectionError("Rate limit — retrying")

        response.raise_for_status()
        return response.json()

    def fetch_all(
        self,
        year: str,
        dataset: Literal["part_a", "part_b"] = "part_b",
    ) -> pd.DataFrame:
        """
        Fetch all pages for a given year and dataset type.

        Args:
            year: 4-digit year string e.g. "2022"
            dataset: "part_a" (inpatient) or "part_b" (physician/supplier)

        Returns:
            pd.DataFrame of all records
        """
        dataset_id = CMS_DATASET_IDS[dataset]
        url = self.BASE_URL.format(dataset_id=dataset_id)
        all_records: list[dict] = []

        for page in range(self.max_pages):
            params = {
                "filter[year]": year,
                "size": PAGE_SIZE,
                "offset": page * PAGE_SIZE,
            }
            logger.info(f"Fetching {dataset} year={year} page={page} offset={page * PAGE_SIZE}")
            records = self._fetch_page(url, params)

            if not records:
                logger.info(f"No more records at page {page} — stopping pagination")
                break

            all_records.extend(records)
            logger.info(f"  → {len(records)} records (total so far: {len(all_records):,})")

            # CMS returns fewer than PAGE_SIZE on the last page
            if len(records) < PAGE_SIZE:
                break

        if not all_records:
            raise ValueError(f"No records returned for {dataset} year={year}")

        df = pd.DataFrame(all_records)
        logger.info(f"Fetched {len(df):,} total rows for {dataset} year={year}")
        return df

    def get_expected_schema(self, dataset: Literal["part_a", "part_b"]) -> set[str]:
        return EXPECTED_SCHEMAS[dataset]
