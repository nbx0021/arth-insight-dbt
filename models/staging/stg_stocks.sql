with raw as (
    select * from {{ source('arth_bq', 'stock_intelligence_v3') }}
),

cleaned as (
    select
        -- 1. Identifiers (Must match exactly)
        ticker,
        company_name,
        
        -- Sector Logic (Mapped to 'sector' for Django)
        CASE 
            WHEN ticker IN ('ITC', 'HINDUNILVR', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'GODREJCP', 'MARICO', 'VBL', 'VARUN BEVERAGES', 'COLPAL') THEN 'FMCG'
            WHEN ticker IN ('MARUTI', 'TATAMOTORS', 'M&M', 'ASHOKLEY', 'HEROMOTOCO', 'EICHERMOT', 'BAJAJ-AUTO', 'TVSMOTOR') THEN 'AUTOMOBILE'
            WHEN ticker IN ('SHRIRAMFIN', 'BAJFINANCE', 'BAJAJFINSV', 'CHOLAFIN', 'MUTHOOTFIN', 'JIOFIN', 'PFC', 'REC', 'IRFC', 'M&MFIN') THEN 'NBFC' 
            WHEN ticker IN ('SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'LUPIN', 'APOLLOHOSP', 'TORNTPHARM', 'ALKEM') THEN 'PHARMA'
            WHEN ticker IN ('TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'VEDL', 'COALINDIA', 'NMDC', 'SAIL', 'HINDZINC') THEN 'METALS'
            WHEN ticker IN ('ULTRACEMCO', 'AMBUJACEM', 'ACC', 'SHREECEM', 'DALBHARAT', 'RAMCOCEM') THEN 'CEMENT'
            WHEN ticker IN ('HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'AXISBANK', 'INDUSINDBK', 'BANKBARODA', 'PNB') THEN 'BANKING'
            WHEN ticker IN ('RELIANCE', 'ONGC', 'OIL', 'IOC', 'BPCL', 'HPCL', 'GAIL', 'IGL', 'MGL') THEN 'OIL & GAS'
            WHEN ticker IN ('INDIGO', 'SPICEJET', 'JETAIRWAYS') THEN 'AVIATION'
            WHEN ticker IN ('ADANIPORTS', 'GPPL', 'JSWINFRA', 'IRB', 'GMRINFRA', 'L&T', 'LT', 'RVNL', 'IRCON') THEN 'INFRASTRUCTURE'
            WHEN ticker IN ('SUZLON', 'INOXWIND', 'KPIGREEN', 'ADANIGREEN', 'TATAPOWER', 'NTPC', 'POWERGRID', 'SJVN', 'NHPC', 'ADANIPOWER') THEN 'POWER & RENEWABLES'
            WHEN ticker IN ('ZOMATO', 'PAYTM', 'NYKAA', 'POLICYBZR', 'DELHIVERY', 'NAUKRI', 'CARTRADE') THEN 'CONSUMER TECH'
            WHEN ticker IN ('INFY', 'TCS', 'WIPRO', 'HCLTECH', 'TECHM', 'LTIM', 'PERSISTENT', 'COFORGE', 'MPHASIS', 'KPITTECH') THEN 'IT SERVICES'
            ELSE sector 
        END as sector,
        
        -- 2. Price Data (Must match lines 166-167 of views.py)
        price as price,
        day_high,
        day_low,
        -- ⚡ FIX: Backticks for columns starting with numbers
        -- Wrap BOTH sides in backticks
        `52_high` as `52_high`,  
        `52_low` as `52_low`,
        -- 🧠 UNPACKING THE JSON (The Missing Link)
        -- We extract these as numbers so the Gold model can do math on them
        CAST(JSON_VALUE(shareholding, '$.promoter') AS FLOAT64) as promoter_holding,
        CAST(JSON_VALUE(shareholding, '$.institution') AS FLOAT64) as institution_holding,
        
        -- 3. Valuation & Financials (Must match lines 167-168 of views.py)
        mcap as mcap,
        enterprise_value,
        pe_ratio,
        industry_pe,
        pb_ratio,
        div_yield,
        coalesce(roe, 0) as roe,
        debt_to_equity,
        profit_growth,
        cagr_5y,
        
        -- 4. Application JSON Objects (Must match lines 174 of views.py)
        technicals,
        news,
        policy,
        shareholding,
        shares_fmt,
        chart_dates,
        chart_prices,
        chart_volumes,

        -- 5. Metadata
        last_updated

    from raw
)

select * from cleaned