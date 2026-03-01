{{ 
    config(
        materialized='incremental',
        unique_key='ticker'
    ) 
}}

with stocks as (
    select * from {{ ref('stg_stocks') }}
    
    {% if is_incremental() %}
        where last_updated > (select max(last_updated) from {{ this }})
    {% endif %}
),

logic_layer as (
    select 
        stocks.*,
        CAST(JSON_EXTRACT_SCALAR(technicals, '$.rsi.val') AS FLOAT64) as rsi_val,
        JSON_EXTRACT_SCALAR(technicals, '$.trend.status') as trend_status,
        JSON_EXTRACT_SCALAR(policy, '$.insights.budget') as budget_stance
    from stocks
),

scored_stocks as (
    select
        *,
        (
            -- 1. FINANCIAL STRENGTH (Max 35 Points)
            -- ROE (Sector Adjusted)
            (CASE 
                WHEN sector IN ('BANKING', 'NBFC', 'FINANCE') THEN 
                    (CASE WHEN roe > 15 THEN 15 WHEN roe > 12 THEN 10 ELSE 0 END)
                WHEN sector IN ('POWER & RENEWABLES', 'INFRASTRUCTURE', 'OIL & GAS', 'METALS', 'CEMENT') THEN
                    (CASE WHEN roe > 12 THEN 15 WHEN roe > 8 THEN 10 ELSE 0 END)
                ELSE 
                    (CASE WHEN roe > 20 THEN 15 WHEN roe > 15 THEN 10 ELSE 0 END)
            END) +
            
            -- Profit Growth
            (CASE WHEN profit_growth > 15 THEN 10 WHEN profit_growth > 0 THEN 5 ELSE 0 END) +

            -- Debt Health (Sector Adjusted)
            (CASE 
                WHEN sector IN ('BANKING', 'NBFC', 'FINANCE') THEN 10
                WHEN sector IN ('POWER & RENEWABLES', 'INFRASTRUCTURE', 'OIL & GAS', 'METALS', 'CEMENT', 'TELECOM') THEN 
                    (CASE WHEN debt_to_equity < 1.5 THEN 10 WHEN debt_to_equity < 2.5 THEN 5 ELSE 0 END)
                ELSE 
                    (CASE WHEN debt_to_equity < 0.5 THEN 10 WHEN debt_to_equity < 1.0 THEN 5 ELSE 0 END)
            END) +

            -- 2. STABILITY & OWNERSHIP (Max 25 Points)
            (CASE WHEN promoter_holding > 50 THEN 10 WHEN promoter_holding > 30 THEN 5 ELSE 0 END) +
            (CASE WHEN mcap > 100000 THEN 10 WHEN mcap > 20000 THEN 5 ELSE 0 END) +
            (CASE WHEN div_yield > 0 THEN 5 ELSE 0 END) +

            -- 3. TECHNICALS (Max 20 Points)
            (CASE WHEN rsi_val > 40 AND rsi_val < 70 THEN 10 ELSE 0 END) +
            (CASE WHEN trend_status LIKE '%Bullish%' THEN 10 WHEN trend_status LIKE '%Bearish%' THEN -5 ELSE 5 END) +

            -- 4. MACRO / POLICY (Max 20 Points)
            (CASE WHEN LOWER(budget_stance) LIKE '%positive%' THEN 20 WHEN LOWER(budget_stance) LIKE '%neutral%' THEN 10 ELSE 0 END)
            
        ) as arth_score

    from logic_layer
),

final_verdict as (
    select 
        *,
        CASE 
            WHEN arth_score >= 75 THEN 'STRONG BUY'
            WHEN arth_score >= 55 THEN 'BUY'
            WHEN arth_score >= 40 THEN 'HOLD'
            ELSE 'AVOID'
        END as verdict
    from scored_stocks
)

select * from final_verdict