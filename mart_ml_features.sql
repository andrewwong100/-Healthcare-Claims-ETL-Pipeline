-- mart_ml_features.sql
-- ML-ready flat feature table at provider × specialty × year grain.
-- Consumed by cost prediction and anomaly detection models.

with provider_stats as (
    select
        provider_npi,
        provider_name,
        provider_specialty,
        provider_state,
        claim_year,

        -- Volume features
        count(distinct hcpcs_code)                              as procedure_diversity,
        sum(total_services)                                     as total_services,
        sum(total_beneficiaries)                                as total_beneficiaries,
        avg(total_services)                                     as avg_services_per_procedure,

        -- Payment features
        avg(avg_medicare_payment)                               as avg_payment,
        stddev(avg_medicare_payment)                            as stddev_payment,
        min(avg_medicare_payment)                               as min_payment,
        max(avg_medicare_payment)                               as max_payment,
        avg(avg_medicare_allowed_amount)                        as avg_allowed_amount,
        avg(avg_submitted_charge)                               as avg_submitted_charge,

        -- Derived: markup ratio (submitted vs allowed — high ratio = billing outlier signal)
        case
            when avg(avg_medicare_allowed_amount) > 0
            then avg(avg_submitted_charge) / avg(avg_medicare_allowed_amount)
            else null
        end                                                     as markup_ratio,

        -- Cost per beneficiary
        case
            when sum(total_beneficiaries) > 0
            then sum(total_services * avg_medicare_payment) / sum(total_beneficiaries)
            else null
        end                                                     as cost_per_beneficiary

    from {{ ref('stg_claims_partb') }}
    group by 1,2,3,4,5
),

specialty_benchmarks as (
    select
        provider_specialty,
        claim_year,
        avg(avg_payment)            as specialty_avg_payment,
        stddev(avg_payment)         as specialty_stddev_payment,
        percentile_cont(0.5)
            within group (order by avg_payment)  as specialty_median_payment
    from provider_stats
    group by 1,2
),

yoy as (
    -- Year-over-year volume and payment change per provider
    select
        curr.provider_npi,
        curr.claim_year,
        curr.total_services                                     as curr_total_services,
        prev.total_services                                     as prev_total_services,
        case
            when prev.total_services > 0
            then (curr.total_services - prev.total_services)::float / prev.total_services
            else null
        end                                                     as yoy_service_growth,
        case
            when prev.avg_payment > 0
            then (curr.avg_payment - prev.avg_payment) / prev.avg_payment
            else null
        end                                                     as yoy_payment_growth
    from provider_stats curr
    left join provider_stats prev
        on curr.provider_npi = prev.provider_npi
        and curr.claim_year  = prev.claim_year + 1
),

final as (
    select
        -- Keys
        ps.provider_npi,
        ps.provider_name,
        ps.provider_specialty,
        ps.provider_state,
        ps.claim_year,

        -- Volume features
        ps.procedure_diversity,
        ps.total_services,
        ps.total_beneficiaries,
        ps.avg_services_per_procedure,

        -- Payment features
        ps.avg_payment,
        ps.stddev_payment,
        ps.min_payment,
        ps.max_payment,
        ps.avg_allowed_amount,
        ps.markup_ratio,
        ps.cost_per_beneficiary,

        -- Specialty-relative features (key for anomaly detection)
        sb.specialty_avg_payment,
        sb.specialty_median_payment,
        case
            when sb.specialty_stddev_payment > 0
            then (ps.avg_payment - sb.specialty_avg_payment) / sb.specialty_stddev_payment
            else null
        end                                                     as payment_z_score,

        -- YoY features
        yoy.yoy_service_growth,
        yoy.yoy_payment_growth,

        -- Label: high-cost outlier (|z-score| > 2 within specialty)
        case
            when abs(
                (ps.avg_payment - sb.specialty_avg_payment)
                / nullif(sb.specialty_stddev_payment, 0)
            ) > 2 then true
            else false
        end                                                     as is_high_cost_outlier,

        -- Audit
        current_timestamp                                       as dbt_updated_at

    from provider_stats ps
    left join specialty_benchmarks sb
        on ps.provider_specialty = sb.provider_specialty
        and ps.claim_year = sb.claim_year
    left join yoy
        on ps.provider_npi = yoy.provider_npi
        and ps.claim_year  = yoy.claim_year
)

select * from final
