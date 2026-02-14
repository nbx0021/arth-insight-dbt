{{ config(materialized='table') }}

with stocks as (
    select * from {{ ref('stg_stocks') }}
),

logic_layer as (
    select 
        stocks.*,

        -- 1. Extract JSON Data for Scoring (The Python equivalent of technicals.get('rsi'))
        -- We cast to FLOAT64 or STRING so we can use them in the CASE statements below
        CAST(JSON_EXTRACT_SCALAR(technicals, '$.rsi.val') AS FLOAT64) as rsi_val,
        JSON_EXTRACT_SCALAR(technicals, '$.trend.status') as trend_status,
        JSON_EXTRACT_SCALAR(policy, '$.insights.budget') as budget_stance
    from stocks
),

scored_stocks as (
    select
        *,
        
        -- 🐍 PYTHON LOGIC PORT: calculate_arth_score
        (
            -- 1. FINANCIALS (Max 40 Points)
            -- ROE Logic: >20(+15), >15(+10), >10(+5)
            (CASE WHEN roe > 20 THEN 15 WHEN roe > 15 THEN 10 WHEN roe > 10 THEN 5 ELSE 0 END) +
            
            -- Profit Growth Logic: >20(+15), >10(+10), >0(+5)
            (CASE WHEN profit_growth > 20 THEN 15 WHEN profit_growth > 10 THEN 10 WHEN profit_growth > 0 THEN 5 ELSE 0 END) +

            -- Debt Logic (Smart Fix for Banks/Power)
            (CASE 
                -- "If 'BANK' or 'FINANCE' or 'POWER' or 'INFRA' in sector..."
                WHEN refined_sector LIKE '%BANK%' OR refined_sector LIKE '%NBFC%' OR refined_sector LIKE '%POWER%' OR refined_sector LIKE '%INFRA%' THEN 
                    (CASE WHEN roe > 10 THEN 10 ELSE 0 END)
                -- "Normal Companies: Penalize High Debt"
                ELSE 
                    (CASE WHEN debt_to_equity < 0.5 THEN 10 WHEN debt_to_equity < 1.0 THEN 5 ELSE 0 END)
            END) +

            -- 2. TECHNICALS (Max 30 Points)
            -- RSI Logic: 35 < rsi < 70 (+10)
            (CASE WHEN rsi_val > 35 AND rsi_val < 70 THEN 10 ELSE 0 END) +
            
            -- Trend Logic: "Bullish"(+20), "Bearish"(-5), Else(+5)
            (CASE 
                WHEN trend_status LIKE '%Bullish%' THEN 20 
                WHEN trend_status LIKE '%Bearish%' THEN -5 
                ELSE 5 
            END) +

            -- 3. POLICY (Max 20 Points)
            -- Budget Logic: "positive"(+20), "neutral"(+10)
            (CASE 
                WHEN LOWER(budget_stance) LIKE '%positive%' THEN 20 
                WHEN LOWER(budget_stance) LIKE '%neutral%' THEN 10 
                ELSE 0 
            END)
            
            -- Note: We cannot do "News Sentiment" (TextBlob) in SQL easily. 
            -- For now, this component is 0, matching the behavior if news is empty.

        ) as arth_score

    from logic_layer
),

final_verdict as (
    select 
        *,
        -- 🐍 PYTHON LOGIC PORT: Verdict Labels
        CASE 
            WHEN arth_score >= 80 THEN 'STRONG BUY'
            WHEN arth_score >= 60 THEN 'BUY'
            WHEN arth_score >= 40 THEN 'HOLD'
            ELSE 'AVOID'
        END as verdict
    from scored_stocks
)

select * from final_verdict