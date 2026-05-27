# 📂 What Files to Include in Your Claims Pipeline Repo

A practical guide to what each file should contain — organized by folder.

---

## `airflow/dags/cms_ingestion_dag.py`

Your main DAG file. Should contain:
- A `DAG` definition with `schedule_interval="@daily"`
- Tasks: `extract_from_cms_api` → `upload_to_s3` → `copy_to_redshift` → `run_dbt_models` → `run_dbt_tests`
- Use `PythonOperator` or `BashOperator` for each step
- Airflow connections referenced by name (not hardcoded credentials)

---

## `ingestion/cms_api_client.py`

The CMS API wrapper. Should include:
- A `CMSApiClient` class with `fetch_page(year, page)` and `fetch_all(year)` methods
- Pagination logic (CMS paginates in chunks of 500–5000 rows)
- Retry logic with exponential backoff (`tenacity` library)
- Rate limiting (respect CMS API limits)
- Returns a list of dicts or a pandas DataFrame

```python
# Example skeleton
class CMSApiClient:
    BASE_URL = "https://data.cms.gov/data-api/v1/dataset/{id}/data"

    def fetch_all(self, year: int) -> pd.DataFrame:
        ...
```

---

## `ingestion/schema_validator.py`

Catches schema drift before it breaks your pipeline. Should include:
- Expected column list and dtypes per dataset version
- A `validate(df)` function that raises or logs on unexpected columns
- A `detect_drift(df, expected_schema)` that returns added/removed fields
- Save a schema snapshot to S3 or local disk on each run for comparison

---

## `dbt/models/staging/stg_claims_partA.sql`

Thin cleaning layer — no joins, no business logic. Should:
- `SELECT` from `{{ source('raw', 'cms_claims_part_a') }}`
- Rename columns to snake_case
- Cast types (e.g. `CAST(total_payment AS DECIMAL(18,2))`)
- Filter obvious garbage rows (`WHERE claim_id IS NOT NULL`)
- Add a `loaded_at` timestamp

---

## `dbt/models/intermediate/int_claims_joined.sql`

Business logic layer. Should:
- Join `stg_claims_partA` + `stg_claims_partB` on shared keys
- Join to `stg_providers` on NPI
- Compute derived fields (e.g. `payment_variance = allowed_amount - actual_payment`)
- No aggregation yet — keep it row-level

---

## `dbt/models/marts/mart_provider_utilization.sql`

The main analytics table. Should aggregate to provider level:
- `provider_npi`, `provider_name`, `specialty`, `state`
- `total_claims`, `total_allowed_amount`, `avg_payment_per_claim`
- `unique_procedures`, `rank_in_specialty`
- Useful for BI dashboards and benchmarking

---

## `dbt/models/marts/mart_ml_features.sql`

ML-ready flat table — **this is what makes your project stand out**. Should include:
- All provider attributes (NPI, specialty, state, years active)
- Aggregated claim stats (volume, avg cost, stddev cost)
- Derived features: `yoy_volume_change`, `cost_per_beneficiary`, `procedure_diversity_score`
- A `label` column if doing supervised learning (e.g. `is_high_cost_outlier`)
- Document every feature in `docs/data_dictionary.md`

---

## `dbt/tests/assert_row_count_threshold.sql`

Custom dbt test. Example:
```sql
-- Fails if row count drops below 800,000 (signals ingestion failure)
SELECT COUNT(*) as row_count
FROM {{ ref('stg_claims_partA') }}
HAVING COUNT(*) < 800000
```

---

## `sql/analysis/provider_outlier_detection.sql`

Standalone analysis query showing insight. Example:
```sql
-- Providers with avg payment > 2 std deviations above specialty mean
WITH stats AS (
    SELECT specialty,
           AVG(avg_payment_per_claim) AS mean_payment,
           STDDEV(avg_payment_per_claim) AS std_payment
    FROM mart_provider_utilization
    GROUP BY specialty
)
SELECT p.*, s.mean_payment,
       (p.avg_payment_per_claim - s.mean_payment) / s.std_payment AS z_score
FROM mart_provider_utilization p
JOIN stats s USING (specialty)
WHERE ABS((p.avg_payment_per_claim - s.mean_payment) / s.std_payment) > 2
ORDER BY z_score DESC
```

---

## `notebooks/eda_claims_exploration.ipynb`

Exploratory Jupyter notebook. Include:
- Dataset shape, null counts, dtype summary
- Distribution of `total_payment` (log scale)
- Top 20 procedures by volume and cost
- Geographic heatmap by state (use `plotly` or `folium`)
- Correlation matrix of numeric features
- 2–3 interesting findings written up as markdown cells

---

## `tests/test_cms_api_client.py`

Unit tests for ingestion code. Include:
- Mock the CMS API using `unittest.mock` or `pytest-httpx`
- Test: successful fetch returns expected DataFrame shape
- Test: pagination loops correctly
- Test: retry fires on 429/500 responses
- Test: schema validator raises on missing required columns

---

## `docs/data_dictionary.md`

Field-level documentation for every mart. Format:

| Field | Type | Description | Example |
|---|---|---|---|
| `provider_npi` | VARCHAR(10) | National Provider Identifier | `1234567890` |
| `avg_payment_per_claim` | DECIMAL(18,2) | Mean Medicare payment across all claims for this provider | `$142.50` |
| `is_high_cost_outlier` | BOOLEAN | True if provider z-score > 2 within specialty | `true` |

---

## `docs/pipeline_runbook.md`

Operations guide. Include:
- How to trigger a backfill for a missed date
- What to do when CMS API is down (fallback strategy)
- How to re-run a failed dbt model
- Monitoring: what CloudWatch alarms or Airflow alerts are set up
- How to add a new CMS dataset to the pipeline

---

## `.github/workflows/ci.yml`

CI pipeline. Should run on every PR:
```yaml
- name: Lint Python
  run: flake8 ingestion/ tests/

- name: Run unit tests
  run: pytest tests/ -v

- name: dbt compile check
  run: cd dbt && dbt compile
```

---
