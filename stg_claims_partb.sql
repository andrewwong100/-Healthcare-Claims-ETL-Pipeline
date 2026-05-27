-- stg_claims_partb.sql
-- Staging layer: clean and type-cast raw CMS Part B (physician/supplier) claims.
-- No business logic here — only renaming, casting, and null filtering.

with source as (
    select * from {{ source('cms_raw', 'cms_claims_part_b') }}
),

renamed as (
    select
        -- Provider identifiers
        trim(rndrng_npi)                                        as provider_npi,
        trim(rndrng_prvdr_last_org_name)                        as provider_name,
        trim(rndrng_prvdr_type)                                 as provider_specialty,
        trim(rndrng_prvdr_state_abrvtn)                         as provider_state,
        trim(rndrng_prvdr_city)                                 as provider_city,

        -- Procedure / service
        trim(hcpcs_cd)                                          as hcpcs_code,
        trim(hcpcs_desc)                                        as hcpcs_description,
        trim(hcpcs_drug_ind)                                    as is_drug_indicator,

        -- Volume metrics
        cast(tot_benes       as integer)                        as total_beneficiaries,
        cast(tot_srvcs       as integer)                        as total_services,
        cast(tot_bene_day_srvcs as integer)                     as total_beneficiary_day_services,

        -- Payment metrics
        cast(avg_mdcr_alowd_amt   as decimal(18,2))             as avg_medicare_allowed_amount,
        cast(avg_submtd_chrg      as decimal(18,2))             as avg_submitted_charge,
        cast(avg_mdcr_pymt_amt    as decimal(18,2))             as avg_medicare_payment,
        cast(avg_mdcr_stdzd_amt   as decimal(18,2))             as avg_medicare_standardized_amount,

        -- Year (added by ingestion pipeline)
        cast(year as integer)                                   as claim_year,

        -- Audit
        current_timestamp                                       as dbt_loaded_at

    from source
),

cleaned as (
    select *
    from renamed
    where
        provider_npi is not null
        and hcpcs_code is not null
        and total_services > 0
        and avg_medicare_payment >= 0
        and claim_year between 2013 and extract(year from current_date)
)

select * from cleaned
